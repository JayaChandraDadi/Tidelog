package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import tidelog.store.InMemoryObjectStore;

class TableTest {

  @Test
  void createAppendAndScan() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    assertEquals(0, t.snapshot().version());
    assertEquals(1, t.append(TestData.rows(0, 100)));
    assertEquals(2, t.append(TestData.rows(100, 50)));
    Snapshot s = t.snapshot();
    assertEquals(2, s.version());
    assertEquals(150, s.numRecords());
    assertEquals(2, s.files().size());
    Set<Long> ids = new HashSet<>();
    for (Row r : s.readAll()) ids.add((Long) r.get(0));
    assertEquals(150, ids.size());
  }

  @Test
  void parquetRoundTripKeepsTypesAndNulls() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    List<Row> rows = TestData.rows(0, 30);
    t.append(rows);
    assertEquals(rows, t.snapshot().readAll());
    var stats = t.snapshot().files().iterator().next().stats();
    assertEquals(0L, ((Number) stats.get("id").min()).longValue());
    assertEquals(29L, ((Number) stats.get("id").max()).longValue());
    assertEquals(5, stats.get("flag").nullCount());  // ids 0, 7, 14, 21, 28
  }

  @Test
  void largeAppendsAreSplitIntoFiles() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA).withMaxRowsPerFile(1000);
    t.append(TestData.rows(0, 4500));
    assertEquals(5, t.snapshot().files().size());
    assertEquals(4500, t.snapshot().numRecords());
  }

  @Test
  void deleteRewritesOnlyAffectedFiles() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    t.append(TestData.rows(0, 100));    // file A: ids 0..99
    t.append(TestData.rows(1000, 100)); // file B: ids 1000..1099
    long v = t.delete(Filter.parse("id < 10"));
    Snapshot s = t.snapshot();
    assertEquals(3, v);
    assertEquals(190, s.numRecords());
    // File B is untouched; file A is replaced by a rewritten copy.
    var ops = t.history().get(3);
    assertEquals("DELETE", ops.operation());
    assertEquals(1, ops.filesRemoved());
    assertEquals(1, ops.filesAdded());
    assertTrue(s.scan(Filter.parse("id < 10")).rows().isEmpty());
  }

  @Test
  void rejectsBadInput() {
    InMemoryObjectStore store = new InMemoryObjectStore();
    Table t = Table.create(store, TestData.SCHEMA);
    assertThrows(IllegalStateException.class, () -> Table.create(store, TestData.SCHEMA));
    assertThrows(IllegalArgumentException.class, () -> t.append(List.of(new Row(1L, "x"))));
    assertThrows(IllegalArgumentException.class, () -> t.append(List.of(new Row(1L, null, 1.0, true))));
    assertThrows(IllegalStateException.class, () -> Table.open(new InMemoryObjectStore()));
  }

  @Test
  void filterParsingAndEvaluation() {
    Filter f = Filter.parse("region = 'EAST' AND (amount >= 5.5 OR id BETWEEN 10 AND 12) AND NOT id = 11");
    Schema s = TestData.SCHEMA;
    assertTrue(f.test(s, new Row(10L, "EAST", 1.0, true)));
    assertFalse(f.test(s, new Row(11L, "EAST", 1.0, true)));
    assertTrue(f.test(s, new Row(99L, "EAST", 6.0, true)));
    assertFalse(f.test(s, new Row(99L, "WEST", 6.0, true)));
    assertThrows(IllegalArgumentException.class, () -> Filter.parse("id >"));
    assertThrows(IllegalArgumentException.class, () -> Filter.parse("id ~ 3"));
  }
}
