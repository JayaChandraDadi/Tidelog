package tidelog;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Row predicates. Besides evaluating rows, a filter can decide from a file's
 * min/max statistics that the file cannot contain a match ({@link #mightMatch}),
 * which is how scans skip files.
 */
public sealed interface Filter permits Filter.Cmp, Filter.And, Filter.Or, Filter.Not {

  boolean test(Schema schema, Row row);

  /** Conservative: returns false only if no row in a file with these stats can match. */
  boolean mightMatch(Map<String, ColumnRange> stats);

  enum Op { EQ, NE, LT, LE, GT, GE }

  record Cmp(String column, Op op, Object literal) implements Filter {
    @Override
    public boolean test(Schema schema, Row row) {
      int i = schema.indexOf(column);
      if (i < 0) throw new IllegalArgumentException("unknown column " + column);
      Object v = row.get(i);
      if (v == null) return false;
      int c = Values.compare(v, literal);
      return switch (op) {
        case EQ -> c == 0;
        case NE -> c != 0;
        case LT -> c < 0;
        case LE -> c <= 0;
        case GT -> c > 0;
        case GE -> c >= 0;
      };
    }

    @Override
    public boolean mightMatch(Map<String, ColumnRange> stats) {
      ColumnRange r = stats == null ? null : stats.get(column);
      if (r == null || r.min() == null || r.max() == null) return r == null || r.min() != null;
      int lo = Values.compare(r.min(), literal), hi = Values.compare(r.max(), literal);
      return switch (op) {
        case EQ -> lo <= 0 && hi >= 0;
        case NE -> !(lo == 0 && hi == 0);
        case LT -> lo < 0;
        case LE -> lo <= 0;
        case GT -> hi > 0;
        case GE -> hi >= 0;
      };
    }

    @Override
    public String toString() {
      String lit = literal instanceof String s ? "'" + s.replace("'", "''") + "'" : String.valueOf(literal);
      String sym = switch (op) {
        case EQ -> "=";
        case NE -> "!=";
        case LT -> "<";
        case LE -> "<=";
        case GT -> ">";
        case GE -> ">=";
      };
      return column + " " + sym + " " + lit;
    }
  }

  record And(Filter left, Filter right) implements Filter {
    public boolean test(Schema s, Row r) { return left.test(s, r) && right.test(s, r); }
    public boolean mightMatch(Map<String, ColumnRange> st) { return left.mightMatch(st) && right.mightMatch(st); }
    public String toString() { return "(" + left + " AND " + right + ")"; }
  }

  record Or(Filter left, Filter right) implements Filter {
    public boolean test(Schema s, Row r) { return left.test(s, r) || right.test(s, r); }
    public boolean mightMatch(Map<String, ColumnRange> st) { return left.mightMatch(st) || right.mightMatch(st); }
    public String toString() { return "(" + left + " OR " + right + ")"; }
  }

  record Not(Filter inner) implements Filter {
    public boolean test(Schema s, Row r) { return !inner.test(s, r); }
    public boolean mightMatch(Map<String, ColumnRange> st) { return true; }
    public String toString() { return "NOT " + inner; }
  }

  static Filter cmp(String column, Op op, Object literal) {
    return new Cmp(column, op, literal);
  }

  static Filter between(String column, Object lo, Object hi) {
    return new And(new Cmp(column, Op.GE, lo), new Cmp(column, Op.LE, hi));
  }

  /** Coerces literals to the column types (e.g. 5 to 5.0 for a DOUBLE column). */
  default Filter bind(Schema schema) {
    if (this instanceof Cmp c) return new Cmp(c.column, c.op, schema.field(c.column).type().coerce(c.literal));
    if (this instanceof And a) return new And(a.left.bind(schema), a.right.bind(schema));
    if (this instanceof Or o) return new Or(o.left.bind(schema), o.right.bind(schema));
    Not n = (Not) this;
    return new Not(n.inner.bind(schema));
  }

  /**
   * Parses expressions like {@code ts >= 100 AND (region = 'EAST' OR amount < 2.5)}.
   * Supports = != <> < <= > >=, BETWEEN x AND y, AND, OR, NOT and parentheses.
   */
  static Filter parse(String text) {
    return new Parser(text).parse();
  }

  final class Parser {
    private final List<String> toks = new ArrayList<>();
    private int p = 0;

    Parser(String s) {
      int i = 0;
      while (i < s.length()) {
        char c = s.charAt(i);
        if (Character.isWhitespace(c)) { i++; continue; }
        if (c == '\'') {
          StringBuilder sb = new StringBuilder("'");
          i++;
          while (true) {
            if (i >= s.length()) throw new IllegalArgumentException("unterminated string");
            if (s.charAt(i) == '\'') {
              if (i + 1 < s.length() && s.charAt(i + 1) == '\'') { sb.append('\''); i += 2; continue; }
              i++;
              break;
            }
            sb.append(s.charAt(i++));
          }
          toks.add(sb.toString());
        } else if ("()".indexOf(c) >= 0) {
          toks.add(String.valueOf(c));
          i++;
        } else if ("<>=!".indexOf(c) >= 0) {
          int j = i + 1;
          if (j < s.length() && "<>=".indexOf(s.charAt(j)) >= 0) j++;
          toks.add(s.substring(i, j));
          i = j;
        } else {
          int j = i;
          while (j < s.length() && !Character.isWhitespace(s.charAt(j)) && "()<>=!'".indexOf(s.charAt(j)) < 0) j++;
          toks.add(s.substring(i, j));
          i = j;
        }
      }
    }

    private String peek() { return p < toks.size() ? toks.get(p) : ""; }
    private boolean kw(String k) { return peek().toUpperCase(Locale.ROOT).equals(k); }
    private String next() {
      if (p >= toks.size()) throw new IllegalArgumentException("unexpected end of filter");
      return toks.get(p++);
    }

    Filter parse() {
      Filter f = or();
      if (p != toks.size()) throw new IllegalArgumentException("unexpected token '" + peek() + "'");
      return f;
    }

    private Filter or() {
      Filter f = and();
      while (kw("OR")) { p++; f = new Or(f, and()); }
      return f;
    }

    private Filter and() {
      Filter f = unary();
      while (kw("AND")) { p++; f = new And(f, unary()); }
      return f;
    }

    private Filter unary() {
      if (kw("NOT")) { p++; return new Not(unary()); }
      if (peek().equals("(")) {
        p++;
        Filter f = or();
        if (!next().equals(")")) throw new IllegalArgumentException("expected )");
        return f;
      }
      String col = next();
      if (kw("BETWEEN")) {
        p++;
        Object lo = literal(next());
        if (!kw("AND")) throw new IllegalArgumentException("expected AND in BETWEEN");
        p++;
        return between(col, lo, literal(next()));
      }
      String op = next();
      Op o = switch (op) {
        case "=", "==" -> Op.EQ;
        case "!=", "<>" -> Op.NE;
        case "<" -> Op.LT;
        case "<=" -> Op.LE;
        case ">" -> Op.GT;
        case ">=" -> Op.GE;
        default -> throw new IllegalArgumentException("unknown operator " + op);
      };
      return new Cmp(col, o, literal(next()));
    }

    private static Object literal(String t) {
      if (t.startsWith("'")) return t.substring(1);
      if (t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false")) return Boolean.parseBoolean(t);
      if (t.contains(".") || t.contains("e") || t.contains("E")) return Double.parseDouble(t);
      return Long.parseLong(t);
    }
  }
}
