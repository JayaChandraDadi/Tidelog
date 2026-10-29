package tidelog;

/** Column types supported by Tidelog tables. */
public enum FieldType {
  LONG,
  DOUBLE,
  STRING,
  BOOLEAN;

  /** Converts a value read from JSON or a file into this type's Java representation. */
  public Object coerce(Object v) {
    if (v == null) return null;
    switch (this) {
      case LONG:
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString().trim());
      case DOUBLE:
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString().trim());
      case BOOLEAN:
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString().trim());
      default:
        return v.toString();
    }
  }
}
