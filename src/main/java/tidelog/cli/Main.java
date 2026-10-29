package tidelog.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import tidelog.Field;
import tidelog.Filter;
import tidelog.Row;
import tidelog.ScanResult;
import tidelog.Schema;
import tidelog.Snapshot;
import tidelog.Table;
import tidelog.store.ObjectStore;

/**
 * Command-line interface.
 *
 * <pre>
 *   tidelog create   --table URI --schema "id:long,region:string,amount:double"
 *   tidelog append   --table URI --csv FILE
 *   tidelog import   --table URI FILE.parquet...
 *   tidelog scan     --table URI [--where EXPR] [--version N] [--limit N]
 *   tidelog delete   --table URI --where EXPR
 *   tidelog compact  --table URI [--target-rows N] [--cluster-by COLUMN]
 *   tidelog history  --table URI
 *   tidelog describe --table URI [--version N]
 * </pre>
 *
 * URI is file:///path, s3://bucket/prefix (set TIDELOG_S3_ENDPOINT for MinIO) or a plain path.
 */
public final class Main {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
      return;
    }
    String cmd = args[0];
    Map<String, String> opts = new HashMap<>();
    List<String> positional = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
        opts.put(args[i].substring(2), args[++i]);
      } else {
        positional.add(args[i]);
      }
    }
    String uri = opts.get("table");
    if (uri == null) {
      usage();
      System.exit(2);
    }
    ObjectStore store = ObjectStore.open(uri);
    try {
      switch (cmd) {
        case "create" -> {
          Table.create(store, Schema.parse(required(opts, "schema")));
          System.out.println("created table at version 0");
        }
        case "append" -> {
          Table t = Table.open(store);
          List<Row> rows = readCsv(Path.of(required(opts, "csv")), t.snapshot().schema());
          long v = t.append(rows);
          System.out.printf("appended %d rows in version %d%n", rows.size(), v);
        }
        case "import" -> {
          Table t = Table.open(store);
          List<byte[]> files = new ArrayList<>();
          for (String p : positional) files.add(Files.readAllBytes(Path.of(p)));
          long v = t.importParquet(files);
          System.out.printf("imported %d files in version %d%n", files.size(), v);
        }
        case "scan" -> {
          Table t = Table.open(store);
          Snapshot s = opts.containsKey("version") ? t.snapshotAt(Long.parseLong(opts.get("version"))) : t.snapshot();
          Filter f = opts.containsKey("where") ? Filter.parse(opts.get("where")) : null;
          ScanResult r = s.scan(f);
          int limit = Integer.parseInt(opts.getOrDefault("limit", "20"));
          StringBuilder header = new StringBuilder();
          for (Field fld : s.schema().fields()) header.append(header.length() > 0 ? "\t" : "").append(fld.name());
          System.out.println(header);
          for (int i = 0; i < Math.min(limit, r.rows().size()); i++) {
            Row row = r.rows().get(i);
            StringBuilder sb = new StringBuilder();
            for (int c = 0; c < row.size(); c++) sb.append(c > 0 ? "\t" : "").append(row.get(c));
            System.out.println(sb);
          }
          System.out.printf("%d rows (version %d; files scanned %d of %d, skipped %d)%n", r.rows().size(),
              s.version(), r.filesScanned(), r.filesTotal(), r.filesSkipped());
        }
        case "delete" -> {
          Table t = Table.open(store);
          long v = t.delete(Filter.parse(required(opts, "where")));
          System.out.printf("committed version %d%n", v);
        }
        case "compact" -> {
          Table t = Table.open(store);
          var r = t.compact(Integer.parseInt(opts.getOrDefault("target-rows", "100000")), opts.get("cluster-by"));
          System.out.printf("version %d: replaced %d files with %d%n", r.version(), r.filesRemoved(), r.filesAdded());
        }
        case "history" -> {
          for (Table.HistoryEntry h : Table.open(store).history())
            System.out.printf("%5d  %s  %-28s read=%d blindAppend=%s +%d -%d%n", h.version(),
                Instant.ofEpochMilli(h.timestamp()), h.operation(), h.readVersion(), h.blindAppend(), h.filesAdded(),
                h.filesRemoved());
        }
        case "describe" -> {
          Table t = Table.open(store);
          Snapshot s = opts.containsKey("version") ? t.snapshotAt(Long.parseLong(opts.get("version"))) : t.snapshot();
          System.out.printf("version %d, %d files, %d rows%nschema: %s%n", s.version(), s.files().size(),
              s.numRecords(), s.schema());
        }
        default -> {
          usage();
          System.exit(2);
        }
      }
    } catch (RuntimeException e) {
      System.err.println("error: " + e.getMessage());
      System.exit(1);
    }
  }

  private static String required(Map<String, String> opts, String key) {
    String v = opts.get(key);
    if (v == null) throw new IllegalArgumentException("--" + key + " is required");
    return v;
  }

  /** Reads a CSV file with a header row; columns are matched to the schema by name. */
  static List<Row> readCsv(Path path, Schema schema) throws IOException {
    List<String> lines = Files.readAllLines(path);
    if (lines.isEmpty()) return List.of();
    String[] header = lines.get(0).split(",", -1);
    int[] map = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      map[i] = -1;
      for (int h = 0; h < header.length; h++) if (header[h].trim().equals(schema.fields().get(i).name())) map[i] = h;
    }
    List<Row> rows = new ArrayList<>();
    for (String line : lines.subList(1, lines.size())) {
      if (line.isBlank()) continue;
      String[] cells = line.split(",", -1);
      Object[] values = new Object[schema.size()];
      for (int i = 0; i < schema.size(); i++)
        values[i] = map[i] < 0 || cells[map[i]].isEmpty() ? null : cells[map[i]];
      rows.add(new Row(values).conform(schema));
    }
    return rows;
  }

  private static void usage() {
    System.err.println("usage: tidelog <create|append|import|scan|delete|compact|history|describe> --table URI [options]");
  }
}
