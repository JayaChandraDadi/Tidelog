package tidelog.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import tidelog.Filter;
import tidelog.Row;
import tidelog.ScanResult;
import tidelog.Schema;
import tidelog.Table;
import tidelog.Transaction;
import tidelog.store.InMemoryObjectStore;
import tidelog.store.LocalObjectStore;

/**
 * Reproducible measurements for the README.
 *
 * <ol>
 *   <li>Data skipping: files scanned by selective range queries before and
 *       after clustering compaction, on unsorted (randomly ordered) data.</li>
 *   <li>Concurrent commits: N writers appending to one table on the local file
 *       system; reports throughput, retries and that no commit was lost.</li>
 * </ol>
 *
 * usage: java -cp target/tidelog.jar tidelog.bench.Benchmark [files] [rowsPerFile] [writers]
 */
public final class Benchmark {
  static final Schema SCHEMA = Schema.parse("event_id:long,region:string,amount:double,is_test:boolean");
  static final String[] REGIONS = {"us-east", "us-west", "eu-central", "ap-south"};

  public static void main(String[] args) throws Exception {
    int files = args.length > 0 ? Integer.parseInt(args[0]) : 200;
    int rowsPerFile = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
    int writers = args.length > 2 ? Integer.parseInt(args[2]) : 16;
    dataSkipping(files, rowsPerFile);
    concurrentCommits(writers, 25);
  }

  static List<Row> randomBatch(Random rnd, int n, long keySpace) {
    List<Row> rows = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      rows.add(new Row((long) (rnd.nextDouble() * keySpace), REGIONS[rnd.nextInt(4)], rnd.nextInt(100_000) / 100.0,
          rnd.nextInt(50) == 0));
    return rows;
  }

  static void dataSkipping(int files, int rowsPerFile) {
    final long keySpace = 100_000_000L;
    Random rnd = new Random(7);
    Table t = Table.create(new InMemoryObjectStore(), SCHEMA);
    for (int i = 0; i < files; i++) t.append(randomBatch(rnd, rowsPerFile, keySpace));

    // 50 selective queries, each covering 1% of the key space.
    List<Filter> queries = new ArrayList<>();
    Random q = new Random(99);
    for (int i = 0; i < 50; i++) {
      long lo = (long) (q.nextDouble() * keySpace * 0.99);
      queries.add(Filter.between("event_id", lo, lo + keySpace / 100));
    }
    double[] before = run(t, queries);
    long t0 = System.nanoTime();
    Table.CompactionResult c = t.compact(rowsPerFile, "event_id");
    double compactSec = (System.nanoTime() - t0) / 1e9;
    double[] after = run(t, queries);

    System.out.println("== Data skipping (" + files + " files x " + rowsPerFile + " rows, random key order)");
    System.out.printf("files scanned per query: %.1f -> %.1f of %d (%.1f%% fewer)%n", before[0], after[0], files,
        100 * (1 - after[0] / before[0]));
    System.out.printf("query time: %.1f ms -> %.1f ms (%.1fx faster)%n", before[1], after[1], before[1] / after[1]);
    System.out.printf("clustering compaction: %d files rewritten into %d in %.1f s%n%n", c.filesRemoved(),
        c.filesAdded(), compactSec);
  }

  /** Returns {average files scanned, average milliseconds}; results are checked to be identical. */
  static double[] run(Table t, List<Filter> queries) {
    var snap = t.snapshot();
    for (Filter f : queries.subList(0, 3)) snap.scan(f);  // warm-up
    long files = 0, nanos = 0;
    for (Filter f : queries) {
      long s = System.nanoTime();
      ScanResult r = snap.scan(f);
      nanos += System.nanoTime() - s;
      files += r.filesScanned();
      if (r.rows().size() != snap.scan(f, false).rows().size()) throw new AssertionError("skipping changed results");
    }
    return new double[] {(double) files / queries.size(), nanos / 1e6 / queries.size()};
  }

  static void concurrentCommits(int writers, int commitsEach) throws Exception {
    Path dir = Files.createTempDirectory("tidelog-bench");
    LocalObjectStore store = new LocalObjectStore(dir);
    Table.create(store, SCHEMA);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<int[]>> futures = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int seed = w;
      futures.add(pool.submit(() -> {
        Random rnd = new Random(seed);
        Table t = Table.open(store);
        start.await();
        int attempts = 0;
        for (int c = 0; c < commitsEach; c++) {
          Transaction tx = t.newTransaction("WRITE");
          tx.append(randomBatch(rnd, 100, 1_000_000));
          tx.commit();
          attempts += tx.attempts();
        }
        return new int[] {attempts};
      }));
    }
    long t0 = System.nanoTime();
    start.countDown();
    long attempts = 0;
    for (var f : futures) attempts += f.get()[0];
    double sec = (System.nanoTime() - t0) / 1e9;
    pool.shutdown();
    var snap = Table.open(store).snapshot();
    int commits = writers * commitsEach;
    System.out.println("== Concurrent commits (" + writers + " writers x " + commitsEach + " appends, local FS)");
    System.out.printf("committed %d versions with %d rows in %.2f s (%.0f commits/s)%n", snap.version(),
        snap.numRecords(), sec, commits / sec);
    System.out.printf("commit attempts: %d for %d commits (%.2f per commit); lost commits: %d%n", attempts, commits,
        (double) attempts / commits, commits - snap.version());
  }

  private Benchmark() {}
}
