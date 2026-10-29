package tidelog;

/**
 * Thrown when a transaction cannot commit because a concurrent commit changed
 * data it depends on (for example, both deleted the same file).
 */
public class ConflictException extends RuntimeException {
  public ConflictException(String message) {
    super(message);
  }
}
