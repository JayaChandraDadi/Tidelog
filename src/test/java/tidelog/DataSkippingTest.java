package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import tidelog.store.InMemoryObjectStore;

class DataSkippingTest {

  private static List<Row> sorted(List<Row> rows) {
    return rows.stream().sorted(Comparator.comparing(Row::toString)).toList();
  }

  @Test
  void statsSkipFilesWithoutChangingResults() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    for (int i = 0; i < 20; i++) t.append(TestData.rows(i * 100L, 100));  // ids sorted by file
    Filter f = Filter.parse("id BETWEEN 450 AND 520");
    ScanResult withStats = t.snapshot().scan(f, true);
    ScanResult without = t.snapshot().scan(f, false);
    assertEquals(sorted(without.rows()), sorted(withStats.rows()));
    assertEquals(71, withStats.rows().size());
    assertEquals(2, withStats.filesScanned());
    assertEquals(18, withStats.filesSkipped());
  }

  @Test
  void clusteringMakesRangeFiltersSelective() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    Random rnd = new Random(42);
    for (int i = 0; i < 40; i++) t.append(TestData.randomRows(rnd, 250, 1_000_000));
    Filter f = Filter.parse("id BETWEEN 100000 AND 120000");

    ScanResult before = t.snapshot().scan(f);
    assertEquals(40, before.filesScanned());  // every file spans the whole id range

    Table.CompactionResult r = t.compact(250, "id");
    assertEquals(40, r.filesRemoved());
    assertEquals(40, r.filesAdded());
    ScanResult after = t.snapshot().scan(f);
    assertEquals(sorted(before.rows()), sorted(after.rows()));
    assertTrue(after.filesScanned() <= 3, "scanned " + after.filesScanned());
  }

  @Test
  void compactionMergesSmallFiles() {
    Table t = Table.create(new InMemoryObjectStore(), TestData.SCHEMA);
    for (int i = 0; i < 30; i++) t.append(TestData.rows(i * 10L, 10));
    var before = t.snapshot().readAll();
    Table.CompactionResult r = t.compact(100, null);
    assertEquals(30, r.filesRemoved());
    assertEquals(3, r.filesAdded());
    assertEquals(3, t.snapshot().files().size());
    assertEquals(sorted(before), sorted(t.snapshot().readAll()));
    assertEquals(0, t.compact(100, null).filesRemoved());  // nothing small left
  }
}
