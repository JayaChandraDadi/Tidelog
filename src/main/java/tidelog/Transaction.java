package tidelog;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import tidelog.Actions.Action;
import tidelog.Actions.AddFile;
import tidelog.Actions.CommitInfo;
import tidelog.Actions.RemoveFile;

/**
 * A unit of change against one snapshot, committed with optimistic
 * concurrency control.
 *
 * <p>Data files are written before the commit; the commit itself is a single
 * conditional write of the next log entry. If another writer wins that
 * version, the transaction checks the winning commits for conflicts and, if
 * there are none, retries at the next version. Readers always see a complete
 * snapshot (snapshot isolation).
 *
 * <p>Conflict rules:
 * <ul>
 *   <li>Blind appends never conflict with other appends.</li>
 *   <li>A transaction that removes files conflicts with any concurrent commit
 *       that removed one of the same files.</li>
 *   <li>A delete with a predicate conflicts with concurrent commits that added
 *       files whose statistics could match that predicate (the added rows might
 *       have needed deleting).</li>
 *   <li>Schema/metadata changes conflict with everything.</li>
 * </ul>
 */
public final class Transaction {
  static final int MAX_ATTEMPTS = 1000;

  private final Table table;
  private final Snapshot base;
  private final String operation;
  private final List<AddFile> adds = new ArrayList<>();
  private final Map<String, RemoveFile> removes = new LinkedHashMap<>();
  private final List<Filter> readPredicates = new ArrayList<>();
  private final Map<String, Long> metrics = new LinkedHashMap<>();
  private boolean committed;
  private int attempts;

  Transaction(Table table, Snapshot base, String operation) {
    this.table = table;
    this.base = base;
    this.operation = operation;
  }

  public Snapshot snapshot() {
    return base;
  }

  /** Writes rows as new data files (at most maxRowsPerFile rows each). */
  public void append(List<Row> rows) {
    Schema schema = base.schema();
    List<Row> conformed = new ArrayList<>(rows.size());
    for (Row r : rows) conformed.add(r.conform(schema));
    int per = table.maxRowsPerFile();
    for (int i = 0; i < conformed.size(); i += per)
      addFile(table.writeDataFile(schema, conformed.subList(i, Math.min(i + per, conformed.size()))));
    metrics.merge("rowsWritten", (long) conformed.size(), Long::sum);
  }

  /** Copy-on-write delete: rewrites every file that may contain matching rows. */
  public long delete(Filter filter) {
    Filter f = filter.bind(base.schema());
    readPredicates.add(f);
    long deleted = 0;
    for (AddFile file : base.files()) {
      if (!f.mightMatch(file.stats())) continue;
      List<Row> rows = base.readFile(file);
      List<Row> keep = new ArrayList<>();
      for (Row r : rows) if (!f.test(base.schema(), r)) keep.add(r);
      if (keep.size() == rows.size()) continue;
      deleted += rows.size() - keep.size();
      removeFile(file);
      if (!keep.isEmpty()) addFile(table.writeDataFile(base.schema(), keep));
    }
    metrics.merge("rowsDeleted", deleted, Long::sum);
    return deleted;
  }

  void addFile(AddFile f) {
    adds.add(f);
  }

  void removeFile(AddFile f) {
    if (!base.contains(f.path())) throw new IllegalArgumentException("file not in snapshot: " + f.path());
    removes.put(f.path(), new RemoveFile(f.path(), System.currentTimeMillis()));
  }

  boolean isBlindAppend() {
    return removes.isEmpty() && readPredicates.isEmpty();
  }

  /** Number of commit attempts the last commit() needed (1 = no contention). */
  public int attempts() {
    return attempts;
  }

  /** Commits and returns the new table version. */
  public long commit() {
    if (committed) throw new IllegalStateException("transaction already committed");
    if (adds.isEmpty() && removes.isEmpty()) {
      committed = true;
      return base.version();
    }
    TransactionLog log = table.log();
    long version = base.version() + 1;
    for (attempts = 1; ; attempts++) {
      List<Action> actions = new ArrayList<>();
      actions.add(Action.of(new CommitInfo(System.currentTimeMillis(), operation, base.version(), isBlindAppend(),
          metrics.isEmpty() ? null : metrics)));
      for (RemoveFile r : removes.values()) actions.add(Action.of(r));
      for (AddFile a : adds) actions.add(Action.of(a));
      if (log.tryCommit(version, actions)) {
        committed = true;
        table.afterCommit(version);
        return version;
      }
      if (attempts >= MAX_ATTEMPTS) throw new ConflictException("gave up after " + attempts + " commit attempts");
      long latest = log.latestVersion();
      for (long v = version; v <= latest; v++) checkConflicts(v, log.read(v));
      version = latest + 1;
      backoff();
    }
  }

  private void checkConflicts(long version, List<Action> winner) {
    Set<String> winnerRemoved = new HashSet<>();
    for (Action a : winner) {
      if (a.metadata() != null) throw new ConflictException("table metadata changed in version " + version);
      if (a.remove() != null) winnerRemoved.add(a.remove().path());
    }
    for (String path : removes.keySet())
      if (winnerRemoved.contains(path))
        throw new ConflictException("file " + path + " was concurrently removed in version " + version);
    if (!readPredicates.isEmpty()) {
      for (Action a : winner) {
        if (a.add() == null) continue;
        for (Filter f : readPredicates)
          if (f.mightMatch(a.add().stats()))
            throw new ConflictException("version " + version + " added data that may match " + f);
      }
    }
  }

  private void backoff() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConflictException("interrupted while retrying commit");
    }
  }
}
