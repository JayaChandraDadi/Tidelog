package tidelog.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe in-memory object store, used by tests and benchmarks. */
public final class InMemoryObjectStore implements ObjectStore {
  private static final Map<String, InMemoryObjectStore> NAMED = new ConcurrentHashMap<>();

  private final ConcurrentSkipListMap<String, byte[]> objects = new ConcurrentSkipListMap<>();
  private final AtomicLong gets = new AtomicLong();

  /** Returns a process-wide store for {@code memory://name} URIs. */
  public static InMemoryObjectStore named(String name) {
    return NAMED.computeIfAbsent(name, n -> new InMemoryObjectStore());
  }

  @Override
  public Optional<byte[]> get(String key) {
    gets.incrementAndGet();
    return Optional.ofNullable(objects.get(key));
  }

  @Override
  public void put(String key, byte[] data) {
    objects.put(key, data.clone());
  }

  @Override
  public boolean putIfAbsent(String key, byte[] data) {
    return objects.putIfAbsent(key, data.clone()) == null;
  }

  @Override
  public List<String> list(String prefix) {
    List<String> keys = new ArrayList<>();
    for (String k : objects.tailMap(prefix).keySet()) {
      if (!k.startsWith(prefix)) break;
      keys.add(k);
    }
    return keys;
  }

  @Override
  public void delete(String key) {
    objects.remove(key);
  }

  /** Number of get() calls served (lets tests count object reads). */
  public long getCount() {
    return gets.get();
  }
}
