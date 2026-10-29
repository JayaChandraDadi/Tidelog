package tidelog.store;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Minimal object-storage interface. The only coordination primitive Tidelog
 * needs is {@link #putIfAbsent}: an atomic "create only if the key does not
 * exist yet" write, which serializes commits to the transaction log.
 */
public interface ObjectStore {

  /** Reads an object, or returns empty if it does not exist. */
  Optional<byte[]> get(String key) throws IOException;

  /** Writes (or overwrites) an object. */
  void put(String key, byte[] data) throws IOException;

  /**
   * Atomically creates an object only if no object exists at {@code key}.
   *
   * @return true if this call created the object, false if it already existed
   */
  boolean putIfAbsent(String key, byte[] data) throws IOException;

  /** Lists keys that start with {@code prefix}, in lexicographic order. */
  List<String> list(String prefix) throws IOException;

  void delete(String key) throws IOException;

  /** Opens a store from a URI: {@code file:///path}, {@code s3://bucket/prefix} or {@code memory://name}. */
  static ObjectStore open(String uri) {
    if (uri.startsWith("file://")) return new LocalObjectStore(java.nio.file.Path.of(uri.substring("file://".length())));
    if (uri.startsWith("s3://")) return S3ObjectStore.fromUri(uri);
    if (uri.startsWith("memory://")) return InMemoryObjectStore.named(uri.substring("memory://".length()));
    return new LocalObjectStore(java.nio.file.Path.of(uri));
  }
}
