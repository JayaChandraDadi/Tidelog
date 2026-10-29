package tidelog;

/** Comparison of cell values; numbers compare across Long and Double. */
public final class Values {
  private Values() {}

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int compare(Object a, Object b) {
    if (a instanceof Number x && b instanceof Number y) {
      if (x instanceof Double || y instanceof Double || x instanceof Float || y instanceof Float)
        return Double.compare(x.doubleValue(), y.doubleValue());
      return Long.compare(x.longValue(), y.longValue());
    }
    if (a.getClass() != b.getClass())
      throw new IllegalArgumentException("cannot compare " + a.getClass().getSimpleName() + " with "
          + b.getClass().getSimpleName());
    return ((Comparable) a).compareTo(b);
  }
}
