package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import tidelog.store.InMemoryObjectStore;

class ConflictTest {

  private Table table() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    t.append(TestData.rows(0, 100));  // one file, ids 0..99
    return t;
  }

  @Test
  void concurrentAppendsRebaseWithoutConflict() {
    Table t = table();
    Transaction a = t.newTransaction("WRITE");
    Transaction b = t.newTransaction("WRITE");
    a.append(TestData.rows(100, 10));
    b.append(TestData.rows(200, 10));
    assertEquals(2, a.commit());
    assertEquals(3, b.commit());  // retried at the next version
    assertEquals(2, b.attempts());
    assertEquals(120, t.snapshot().numRecords());
  }

  @Test
  void twoDeletesOfTheSameFileConflict() {
    Table t = table();
    Transaction a = t.newTransaction("DELETE");
    Transaction b = t.newTransaction("DELETE");
    a.delete(Filter.parse("id = 1"));
    b.delete(Filter.parse("id = 2"));
    a.commit();
    assertThrows(ConflictException.class, b::commit);
    assertEquals(99, t.snapshot().numRecords());  // b's change never became visible
  }

  @Test
  void deleteConflictsWithConcurrentAppendOfMatchingRows() {
    Table t = table();
    Transaction del = t.newTransaction("DELETE");
    del.delete(Filter.parse("region = 'EAST'"));
    t.append(TestData.rows(500, 4));  // contains an EAST row the delete never saw
    assertThrows(ConflictException.class, del::commit);
  }

  @Test
  void deleteIgnoresConcurrentAppendOutsideItsPredicate() {
    Table t = table();
    Transaction del = t.newTransaction("DELETE");
    del.delete(Filter.parse("id < 50"));
    t.append(TestData.rows(1000, 10));  // stats show ids 1000..1009: cannot match id < 50
    assertDoesNotThrow(del::commit);
    assertEquals(60, t.snapshot().numRecords());
  }

  @Test
  void compactionDoesNotLoseConcurrentAppends() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    for (int i = 0; i < 10; i++) t.append(TestData.rows(i * 10L, 10));
    // Start compaction from the current snapshot, then let an append win first.
    Transaction compaction = t.newTransaction("OPTIMIZE");
    Snapshot s = compaction.snapshot();
    var rows = new java.util.ArrayList<Row>();
    for (var f : s.files()) {
      rows.addAll(s.readFile(f));
      compaction.removeFile(f);
    }
    compaction.addFile(t.writeDataFile(s.schema(), rows));
    t.append(TestData.rows(100, 5));
    assertDoesNotThrow(compaction::commit);
    assertEquals(105, t.snapshot().numRecords());
    assertEquals(2, t.snapshot().files().size());
  }

  @Test
  void compactionConflictsWithDeleteOfSameFiles() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    for (int i = 0; i < 4; i++) t.append(TestData.rows(i * 10L, 10));
    Transaction compaction = t.newTransaction("OPTIMIZE");
    Snapshot s = compaction.snapshot();
    for (var f : s.files()) compaction.removeFile(f);
    t.delete(Filter.parse("id = 3"));
    assertThrows(ConflictException.class, compaction::commit);
    assertEquals(39, t.snapshot().numRecords());
  }
}
