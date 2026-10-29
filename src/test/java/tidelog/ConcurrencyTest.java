package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tidelog.store.InMemoryObjectStore;
import tidelog.store.LocalObjectStore;
import tidelog.store.ObjectStore;

class ConcurrencyTest {
  static final int WRITERS = 16;
  static final int COMMITS_PER_WRITER = 25;
  static final int ROWS_PER_COMMIT = 40;

  @Test
  void sixteenConcurrentWritersInMemory() throws Exception {
    runWriters(new InMemoryObjectStore());
  }

  @Test
  void sixteenConcurrentWritersOnLocalFileSystem(@TempDir Path dir) throws Exception {
    runWriters(new LocalObjectStore(dir));
  }

  /**
   * Every writer appends through its own Table handle (like separate
   * processes). All commits must land exactly once: no lost or duplicated rows,
   * and versions 1..N are each taken by one commit.
   */
  private void runWriters(ObjectStore store) throws Exception {
    Table.create(store, TestData.SCHEMA);
    ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger retried = new AtomicInteger();
    List<Future<List<Long>>> futures = new ArrayList<>();
    for (int w = 0; w < WRITERS; w++) {
      final int writer = w;
      futures.add(pool.submit(() -> {
        Table t = Table.open(store);
        start.await();
        List<Long> versions = new ArrayList<>();
        for (int c = 0; c < COMMITS_PER_WRITER; c++) {
          long base = ((long) writer * COMMITS_PER_WRITER + c) * ROWS_PER_COMMIT;
          Transaction tx = t.newTransaction("WRITE");
          tx.append(TestData.rows(base, ROWS_PER_COMMIT));
          versions.add(tx.commit());
          if (tx.attempts() > 1) retried.incrementAndGet();
        }
        return versions;
      }));
    }
    start.countDown();
    Set<Long> versions = new HashSet<>();
    for (var f : futures) for (long v : f.get()) assertTrue(versions.add(v), "version " + v + " committed twice");
    pool.shutdown();

    int total = WRITERS * COMMITS_PER_WRITER;
    assertEquals(total, versions.size());
    Snapshot s = Table.open(store).snapshot();
    assertEquals(total, s.version());
    assertEquals((long) total * ROWS_PER_COMMIT, s.numRecords());
    Set<Long> ids = new HashSet<>();
    for (Row r : s.readAll()) assertTrue(ids.add((Long) r.get(0)), "duplicate row " + r);
    assertEquals(total * ROWS_PER_COMMIT, ids.size());
    assertTrue(retried.get() > 0, "expected contention between writers");
  }

  @Test
  void onlyOneConcurrentPutIfAbsentWins(@TempDir Path dir) throws Exception {
    LocalObjectStore store = new LocalObjectStore(dir);
    ExecutorService pool = Executors.newFixedThreadPool(8);
    AtomicInteger winners = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> fs = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      final byte[] payload = ("writer" + i).getBytes();
      fs.add(pool.submit(() -> {
        start.await();
        if (store.putIfAbsent("_tidelog/00000000000000000001.json", payload)) winners.incrementAndGet();
        return null;
      }));
    }
    start.countDown();
    for (var f : fs) f.get();
    pool.shutdown();
    assertEquals(1, winners.get());
    assertTrue(new String(Files.readAllBytes(dir.resolve("_tidelog/00000000000000000001.json"))).startsWith("writer"));
  }
}
