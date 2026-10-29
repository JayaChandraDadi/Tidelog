package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import tidelog.store.InMemoryObjectStore;

class TimeTravelTest {

  @Test
  void readsOldVersionsAfterDeletes() throws Exception {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    t.append(TestData.rows(0, 100));
    long afterAppend = t.snapshot().timestamp();
    Thread.sleep(5);
    t.delete(Filter.parse("region = 'EAST'"));
    assertEquals(75, t.snapshot().numRecords());
    assertEquals(100, t.snapshotAt(1).numRecords());
    assertEquals(0, t.snapshotAt(0).numRecords());
    assertEquals(100, t.snapshotAsOf(afterAppend).numRecords());
    // Old data files are still readable through the old snapshot.
    assertEquals(25, t.snapshotAt(1).scan(Filter.parse("region = 'EAST'")).rows().size());
    assertThrows(IllegalArgumentException.class, () -> t.snapshotAt(9));
  }

  @Test
  void checkpointsMatchFullReplay() {
    InMemoryObjectStore store = new InMemoryObjectStore();
    Table t = Table.create(store, TestData.SCHEMA).withCheckpointInterval(5);
    for (int i = 0; i < 23; i++) {
      t.append(TestData.rows(i * 10L, 10));
      if (i % 4 == 3) t.delete(Filter.parse("id = " + (i * 10L)));
    }
    TransactionLog log = new TransactionLog(store);
    long latest = log.latestVersion();
    assertEquals(List.of(5L, 10L, 15L, 20L, 25L), log.checkpoints());
    for (long v = 0; v <= latest; v++) {
      Snapshot viaCheckpoint = Table.open(store).snapshotAt(v);
      Snapshot viaReplay = Snapshot.replay(store, null, 0, v, log);
      assertEquals(viaReplay.version(), viaCheckpoint.version());
      assertEquals(viaReplay.files().stream().map(f -> f.path()).sorted().toList(),
          viaCheckpoint.files().stream().map(f -> f.path()).sorted().toList(), "version " + v);
    }
  }

  @Test
  void loadingFromCheckpointReadsFewerLogEntries() {
    InMemoryObjectStore store = new InMemoryObjectStore();
    Table t = Table.create(store, TestData.SCHEMA).withCheckpointInterval(10);
    for (int i = 0; i < 49; i++) t.append(TestData.rows(i, 1));
    long before = store.getCount();
    Table.open(store).snapshot();
    long reads = store.getCount() - before;
    // Checkpoint at 40 plus commits 41..49 (and the _last_checkpoint hint), not 50 commit files.
    assertTrue(reads <= 12, "read " + reads + " objects");
  }
}
