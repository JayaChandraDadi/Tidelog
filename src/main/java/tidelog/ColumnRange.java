package tidelog;

/** Per-file column statistics used for data skipping. min/max ignore nulls. */
public record ColumnRange(Object min, Object max, long nullCount) {

  /** Extends the range with one value. */
  public static ColumnRange add(ColumnRange r, Object v) {
    if (r == null) r = new ColumnRange(null, null, 0);
    if (v == null) return new ColumnRange(r.min, r.max, r.nullCount + 1);
    Object min = r.min == null || Values.compare(v, r.min) < 0 ? v : r.min;
    Object max = r.max == null || Values.compare(v, r.max) > 0 ? v : r.max;
    return new ColumnRange(min, max, r.nullCount);
  }

  ColumnRange coerce(FieldType t) {
    return new ColumnRange(t.coerce(min), t.coerce(max), nullCount);
  }
}
