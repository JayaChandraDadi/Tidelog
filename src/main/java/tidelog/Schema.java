package tidelog;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Ordered list of fields. */
public record Schema(List<Field> fields) {
  public Schema {
    fields = List.copyOf(fields);
    for (int i = 0; i < fields.size(); i++)
      for (int j = i + 1; j < fields.size(); j++)
        if (fields.get(i).name().equals(fields.get(j).name()))
          throw new IllegalArgumentException("duplicate field " + fields.get(i).name());
  }

  public int indexOf(String name) {
    for (int i = 0; i < fields.size(); i++) if (fields.get(i).name().equals(name)) return i;
    return -1;
  }

  public Field field(String name) {
    int i = indexOf(name);
    if (i < 0) throw new IllegalArgumentException("unknown column " + name);
    return fields.get(i);
  }

  public int size() {
    return fields.size();
  }

  /** Parses {@code "id:long,region:string,amount:double?"}; a trailing ? marks a nullable column. */
  public static Schema parse(String spec) {
    List<Field> out = new ArrayList<>();
    for (String part : spec.split(",")) {
      String[] kv = part.trim().split(":");
      if (kv.length != 2) throw new IllegalArgumentException("bad field spec: " + part);
      String type = kv[1].trim().toUpperCase(Locale.ROOT);
      boolean nullable = type.endsWith("?");
      if (nullable) type = type.substring(0, type.length() - 1);
      out.add(new Field(kv[0].trim(), FieldType.valueOf(type), nullable));
    }
    return new Schema(out);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Field f : fields) {
      if (sb.length() > 0) sb.append(',');
      sb.append(f.name()).append(':').append(f.type().name().toLowerCase(Locale.ROOT));
      if (f.nullable()) sb.append('?');
    }
    return sb.toString();
  }
}
