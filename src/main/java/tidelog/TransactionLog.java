package tidelog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import tidelog.Actions.Action;
import tidelog.store.ObjectStore;

/**
 * The ordered log under {@code _tidelog/}: one file per committed version
 * ({@code 00000000000000000007.json}) plus periodic checkpoints that hold the
 * full table state so readers do not replay the whole history.
 */
final class TransactionLog {
  static final String DIR = "_tidelog/";
  private static final String LAST_CHECKPOINT = DIR + "_last_checkpoint";

  private final ObjectStore store;

  TransactionLog(ObjectStore store) {
    this.store = store;
  }

  static String commitKey(long version) {
    return DIR + String.format("%020d.json", version);
  }

  static String checkpointKey(long version) {
    return DIR + String.format("%020d.checkpoint.json", version);
  }

  /** Atomically writes commit {@code version}; false if another writer already took it. */
  boolean tryCommit(long version, List<Action> actions) {
    try {
      return store.putIfAbsent(commitKey(version), Json.encodeLines(actions));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  List<Action> read(long version) {
    try {
      byte[] data = store.get(commitKey(version))
          .orElseThrow(() -> new IllegalStateException("missing log entry for version " + version));
      return Json.decodeLines(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Highest committed version, or -1 for a table that does not exist. */
  long latestVersion() {
    long latest = -1;
    for (String key : list()) {
      String name = key.substring(DIR.length());
      if (name.length() == 25 && name.endsWith(".json") && Character.isDigit(name.charAt(0)))
        latest = Math.max(latest, Long.parseLong(name.substring(0, 20)));
    }
    return latest;
  }

  /** Versions that have a checkpoint, ascending. */
  List<Long> checkpoints() {
    List<Long> out = new ArrayList<>();
    for (String key : list()) {
      String name = key.substring(DIR.length());
      if (name.endsWith(".checkpoint.json")) out.add(Long.parseLong(name.substring(0, 20)));
    }
    return out;
  }

  Optional<Long> lastCheckpointHint() {
    try {
      return store.get(LAST_CHECKPOINT).map(b -> Long.parseLong(new String(b).trim()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  void writeCheckpoint(long version, List<Action> state) {
    try {
      store.put(checkpointKey(version), Json.encodeLines(state));
      store.put(LAST_CHECKPOINT, Long.toString(version).getBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  List<Action> readCheckpoint(long version) {
    try {
      return Json.decodeLines(store.get(checkpointKey(version))
          .orElseThrow(() -> new IllegalStateException("missing checkpoint " + version)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<String> list() {
    try {
      return store.list(DIR);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
