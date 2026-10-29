package tidelog;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

final class TestData {
  static final Schema SCHEMA = Schema.parse("id:long,region:string,amount:double,flag:boolean?");
  static final String[] REGIONS = {"EAST", "WEST", "NORTH", "SOUTH"};

  static List<Row> rows(long startId, int n) {
    List<Row> out = new ArrayList<>();
    for (long id = startId; id < startId + n; id++)
      out.add(new Row(id, REGIONS[(int) (id % REGIONS.length)], (id % 1000) / 10.0, id % 7 == 0 ? null : id % 2 == 0));
    return out;
  }

  /** Rows whose ids are a random sample from [0, maxId), like unsorted event data. */
  static List<Row> randomRows(Random rnd, int n, long maxId) {
    List<Row> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      long id = (long) (rnd.nextDouble() * maxId);
      out.add(new Row(id, REGIONS[rnd.nextInt(REGIONS.length)], rnd.nextInt(10000) / 100.0, rnd.nextBoolean()));
    }
    return out;
  }

  private TestData() {}
}
