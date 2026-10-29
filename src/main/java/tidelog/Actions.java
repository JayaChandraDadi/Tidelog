package tidelog;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

/**
 * Entries of the transaction log. Each commit file holds one JSON action per
 * line; a table's state at version N is the replay of commits 0..N.
 */
public final class Actions {
  private Actions() {}

  /** Exactly one field is set per log line. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Action(CommitInfo commitInfo, Metadata metadata, AddFile add, RemoveFile remove) {
    public static Action of(CommitInfo c) { return new Action(c, null, null, null); }
    public static Action of(Metadata m) { return new Action(null, m, null, null); }
    public static Action of(AddFile a) { return new Action(null, null, a, null); }
    public static Action of(RemoveFile r) { return new Action(null, null, null, r); }
  }

  /** Who committed what, and from which snapshot. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record CommitInfo(long timestamp, String operation, long readVersion, boolean isBlindAppend,
                           Map<String, Long> metrics) {}

  /** Table identity and schema (written at version 0). */
  public record Metadata(String id, String schema, long createdTime) {
    public Schema parsedSchema() {
      return Schema.parse(schema);
    }
  }

  /** A data file that becomes part of the table. Path is relative to the table root. */
  public record AddFile(String path, long size, long numRecords, long modificationTime,
                        Map<String, ColumnRange> stats) {}

  /** A data file that is logically deleted (it stays in storage for time travel). */
  public record RemoveFile(String path, long deletionTimestamp) {}
}
