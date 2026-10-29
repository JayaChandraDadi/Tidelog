package tidelog;

import java.util.Arrays;

/** One table row; values are Long, Double, String, Boolean or null, in schema order. */
public final class Row {
  private final Object[] values;

  public Row(Object... values) {
    this.values = values.clone();
  }

  public Object get(int i) {
    return values[i];
  }

  public int size() {
    return values.length;
  }

  /** Validates and normalizes the row against a schema (e.g. Integer to Long). */
  public Row conform(Schema schema) {
    if (values.length != schema.size())
      throw new IllegalArgumentException("row has " + values.length + " values, schema has " + schema.size());
    Object[] out = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      Field f = schema.fields().get(i);
      if (values[i] == null && !f.nullable()) throw new IllegalArgumentException("null in non-nullable " + f.name());
      out[i] = f.type().coerce(values[i]);
    }
    return new Row(out);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Row r && Arrays.equals(values, r.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
