package tidelog;

/** A named, typed column. */
public record Field(String name, FieldType type, boolean nullable) {
  public Field {
    if (name == null || name.isBlank()) throw new IllegalArgumentException("field name must not be empty");
  }
}
