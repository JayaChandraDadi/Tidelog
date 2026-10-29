package tidelog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tidelog.Actions.Action;
import tidelog.Actions.AddFile;
import tidelog.Actions.Metadata;
import tidelog.store.ObjectStore;

/** An immutable view of the table at one version. */
public final class Snapshot {
  private final ObjectStore store;
  private final long version;
  private final long timestamp;
  private final Metadata metadata;
  private final Schema schema;
  private final Map<String, AddFile> files;

  Snapshot(ObjectStore store, long version, long timestamp, Metadata metadata, Map<String, AddFile> files) {
    this.store = store;
    this.version = version;
    this.timestamp = timestamp;
    this.metadata = metadata;
    this.schema = metadata.parsedSchema();
    this.files = Collections.unmodifiableMap(files);
  }

  /** Applies commits on top of a base state. */
  static Snapshot replay(ObjectStore store, Snapshot base, long fromVersion, long toVersion, TransactionLog log) {
    Map<String, AddFile> files = new LinkedHashMap<>(base == null ? Map.of() : base.files);
    Metadata metadata = base == null ? null : base.metadata;
    long ts = base == null ? 0 : base.timestamp;
    for (long v = fromVersion; v <= toVersion; v++) {
      for (Action a : log.read(v)) {
        if (a.metadata() != null) metadata = a.metadata();
        if (a.add() != null) files.put(a.add().path(), a.add());
        if (a.remove() != null) files.remove(a.remove().path());
        if (a.commitInfo() != null) ts = a.commitInfo().timestamp();
      }
    }
    if (metadata == null) throw new IllegalStateException("table has no metadata");
    return new Snapshot(store, toVersion, ts, metadata, files);
  }

  /** Builds a snapshot from checkpoint actions. */
  static Snapshot fromCheckpoint(ObjectStore store, long version, List<Action> actions) {
    Map<String, AddFile> files = new LinkedHashMap<>();
    Metadata metadata = null;
    long ts = 0;
    for (Action a : actions) {
      if (a.metadata() != null) metadata = a.metadata();
      if (a.add() != null) files.put(a.add().path(), a.add());
      if (a.commitInfo() != null) ts = a.commitInfo().timestamp();
    }
    return new Snapshot(store, version, ts, metadata, files);
  }

  /** The full state as actions, as written to a checkpoint. */
  List<Action> toCheckpointActions() {
    List<Action> out = new ArrayList<>();
    out.add(Action.of(new Actions.CommitInfo(timestamp, "CHECKPOINT", version, false, null)));
    out.add(Action.of(metadata));
    for (AddFile f : files.values()) out.add(Action.of(f));
    return out;
  }

  public long version() { return version; }
  public long timestamp() { return timestamp; }
  public Schema schema() { return schema; }
  public Metadata metadata() { return metadata; }
  public Collection<AddFile> files() { return files.values(); }
  public boolean contains(String path) { return files.containsKey(path); }

  public long numRecords() {
    long n = 0;
    for (AddFile f : files.values()) n += f.numRecords();
    return n;
  }

  /** Reads every row. */
  public List<Row> readAll() {
    return scan(null).rows();
  }

  /** Reads the rows matching {@code filter}, skipping files whose statistics rule out a match. */
  public ScanResult scan(Filter filter) {
    return scan(filter, true);
  }

  public ScanResult scan(Filter filter, boolean useStats) {
    Filter f = filter == null ? null : filter.bind(schema);
    List<Row> rows = new ArrayList<>();
    int scanned = 0, skipped = 0;
    for (AddFile file : files.values()) {
      if (f != null && useStats && !f.mightMatch(file.stats())) {
        skipped++;
        continue;
      }
      scanned++;
      for (Row r : readFile(file)) if (f == null || f.test(schema, r)) rows.add(r);
    }
    return new ScanResult(rows, files.size(), scanned, skipped);
  }

  List<Row> readFile(AddFile file) {
    try {
      byte[] data = store.get(file.path())
          .orElseThrow(() -> new IllegalStateException("data file missing: " + file.path()));
      return ParquetIO.read(data, schema);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
