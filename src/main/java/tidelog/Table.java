package tidelog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import tidelog.Actions.Action;
import tidelog.Actions.AddFile;
import tidelog.Actions.CommitInfo;
import tidelog.Actions.Metadata;
import tidelog.store.ObjectStore;

/**
 * A Tidelog table: Parquet data files under {@code data/} plus the transaction
 * log under {@code _tidelog/}, all in one object store.
 */
public final class Table {
  private final ObjectStore store;
  private final TransactionLog log;
  private int checkpointInterval = 10;
  private int maxRowsPerFile = 100_000;
  private volatile Snapshot cached;

  private Table(ObjectStore store) {
    this.store = store;
    this.log = new TransactionLog(store);
  }

  /** Creates a new, empty table (version 0). Fails if a table already exists. */
  public static Table create(ObjectStore store, Schema schema) {
    Table t = new Table(store);
    Metadata md = new Metadata(UUID.randomUUID().toString(), schema.toString(), System.currentTimeMillis());
    List<Action> actions = List.of(
        Action.of(new CommitInfo(System.currentTimeMillis(), "CREATE TABLE", -1, false, null)), Action.of(md));
    if (!t.log.tryCommit(0, actions)) throw new IllegalStateException("table already exists");
    return t;
  }

  /** Opens an existing table. */
  public static Table open(ObjectStore store) {
    Table t = new Table(store);
    if (t.log.latestVersion() < 0) throw new IllegalStateException("no table found");
    return t;
  }

  public Table withCheckpointInterval(int n) {
    this.checkpointInterval = n;
    return this;
  }

  public Table withMaxRowsPerFile(int n) {
    this.maxRowsPerFile = n;
    return this;
  }

  int maxRowsPerFile() {
    return maxRowsPerFile;
  }

  TransactionLog log() {
    return log;
  }

  ObjectStore store() {
    return store;
  }

  /** The latest snapshot. Cached state is extended with only the new commits. */
  public Snapshot snapshot() {
    long latest = log.latestVersion();
    Snapshot c = cached;
    if (c != null && c.version() == latest) return c;
    Snapshot s = (c != null && c.version() < latest) ? Snapshot.replay(store, c, c.version() + 1, latest, log)
                                                     : snapshotAt(latest);
    cached = s;
    return s;
  }

  /** Time travel by version: loads the nearest checkpoint at or below it, then replays. */
  public Snapshot snapshotAt(long version) {
    long latest = log.latestVersion();
    if (version < 0 || version > latest) throw new IllegalArgumentException("version " + version + " does not exist");
    long cp = -1;
    var hint = log.lastCheckpointHint();
    if (hint.isPresent() && hint.get() <= version) {
      cp = hint.get();
    } else {
      for (long v : log.checkpoints()) if (v <= version) cp = Math.max(cp, v);
    }
    if (cp >= 0) {
      Snapshot base = Snapshot.fromCheckpoint(store, cp, log.readCheckpoint(cp));
      return cp == version ? base : Snapshot.replay(store, base, cp + 1, version, log);
    }
    return Snapshot.replay(store, null, 0, version, log);
  }

  /** Time travel by timestamp: the latest version committed at or before {@code millis}. */
  public Snapshot snapshotAsOf(long millis) {
    long best = -1;
    for (HistoryEntry h : history()) if (h.timestamp() <= millis) best = Math.max(best, h.version());
    if (best < 0) throw new IllegalArgumentException("no version at or before " + millis);
    return snapshotAt(best);
  }

  public record HistoryEntry(long version, long timestamp, String operation, long readVersion, boolean blindAppend,
                             int filesAdded, int filesRemoved) {}

  public List<HistoryEntry> history() {
    List<HistoryEntry> out = new ArrayList<>();
    long latest = log.latestVersion();
    for (long v = 0; v <= latest; v++) {
      CommitInfo ci = null;
      int added = 0, removed = 0;
      for (Action a : log.read(v)) {
        if (a.commitInfo() != null) ci = a.commitInfo();
        if (a.add() != null) added++;
        if (a.remove() != null) removed++;
      }
      if (ci != null)
        out.add(new HistoryEntry(v, ci.timestamp(), ci.operation(), ci.readVersion(), ci.isBlindAppend(), added, removed));
    }
    return out;
  }

  public Transaction newTransaction(String operation) {
    return new Transaction(this, snapshot(), operation);
  }

  /** Appends rows in one transaction and returns the committed version. */
  public long append(List<Row> rows) {
    Transaction tx = newTransaction("WRITE");
    tx.append(rows);
    return tx.commit();
  }

  /** Deletes matching rows (copy-on-write) and returns the committed version. */
  public long delete(Filter filter) {
    Transaction tx = newTransaction("DELETE");
    tx.delete(filter);
    return tx.commit();
  }

  public record CompactionResult(long version, int filesRemoved, int filesAdded) {}

  /**
   * Rewrites small files into files of about {@code targetRows} rows. With
   * {@code clusterBy}, all files are rewritten in that column's order so each
   * file covers a narrow value range and range filters skip most files.
   * Concurrent appends do not conflict with compaction.
   */
  public CompactionResult compact(int targetRows, String clusterBy) {
    Transaction tx = newTransaction(clusterBy == null ? "OPTIMIZE" : "OPTIMIZE CLUSTER BY " + clusterBy);
    Snapshot s = tx.snapshot();
    List<AddFile> candidates = new ArrayList<>();
    for (AddFile f : s.files()) if (clusterBy != null || f.numRecords() < targetRows / 2) candidates.add(f);
    if (candidates.size() < 2 && clusterBy == null) return new CompactionResult(s.version(), 0, 0);
    List<Row> rows = new ArrayList<>();
    for (AddFile f : candidates) rows.addAll(s.readFile(f));
    if (clusterBy != null) {
      int idx = s.schema().indexOf(clusterBy);
      if (idx < 0) throw new IllegalArgumentException("unknown column " + clusterBy);
      rows.sort(Comparator.comparing((Row r) -> r.get(idx), Comparator.nullsLast(Values::compare)));
    }
    for (AddFile f : candidates) tx.removeFile(f);
    int added = 0;
    for (int i = 0; i < rows.size(); i += targetRows) {
      tx.addFile(writeDataFile(s.schema(), rows.subList(i, Math.min(i + targetRows, rows.size()))));
      added++;
    }
    long v = tx.commit();
    return new CompactionResult(v, candidates.size(), added);
  }

  /** Registers an existing Parquet file (e.g. written by Spark) by copying it into the table. */
  public long importParquet(List<byte[]> parquetFiles) {
    Transaction tx = newTransaction("IMPORT");
    Schema schema = tx.snapshot().schema();
    for (byte[] data : parquetFiles) {
      Schema fileSchema = ParquetIO.inferSchema(data);
      for (Field f : schema.fields()) {
        int i = fileSchema.indexOf(f.name());
        if (i < 0 && !f.nullable()) throw new IllegalArgumentException("file is missing column " + f.name());
        if (i >= 0 && fileSchema.fields().get(i).type() != f.type())
          throw new IllegalArgumentException("column " + f.name() + " has type " + fileSchema.fields().get(i).type());
      }
      List<Row> rows = ParquetIO.read(data, schema);
      for (Row r : rows) r.conform(schema);
      String path = "data/part-" + UUID.randomUUID() + ".parquet";
      put(path, data);
      tx.addFile(new AddFile(path, data.length, rows.size(), System.currentTimeMillis(),
          ParquetIO.computeStats(schema, rows)));
    }
    return tx.commit();
  }

  AddFile writeDataFile(Schema schema, List<Row> rows) {
    ParquetIO.Written w = ParquetIO.write(schema, rows);
    String path = "data/part-" + UUID.randomUUID() + ".parquet";
    put(path, w.bytes());
    return new AddFile(path, w.bytes().length, w.numRecords(), System.currentTimeMillis(), w.stats());
  }

  private void put(String key, byte[] data) {
    try {
      store.put(key, data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  void afterCommit(long version) {
    if (checkpointInterval > 0 && version % checkpointInterval == 0) {
      log.writeCheckpoint(version, snapshotAt(version).toCheckpointActions());
    }
  }
}
