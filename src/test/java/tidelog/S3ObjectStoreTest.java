package tidelog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import tidelog.store.S3ObjectStore;

/** Runs against MinIO (or any S3 endpoint) when TIDELOG_S3_ENDPOINT is set. */
@EnabledIfEnvironmentVariable(named = "TIDELOG_S3_ENDPOINT", matches = ".+")
class S3ObjectStoreTest {
  static S3Client client;
  static final String BUCKET = "tidelog-test";

  @BeforeAll
  static void setUp() {
    client = S3ObjectStore.clientFromEnv();
    try {
      client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    } catch (Exception alreadyExists) {
      // fine
    }
  }

  private S3ObjectStore store() {
    return new S3ObjectStore(client, BUCKET, "tables/" + UUID.randomUUID());
  }

  @Test
  void conditionalPutIsExclusive() throws Exception {
    S3ObjectStore s = store();
    assertTrue(s.putIfAbsent("k", "a".getBytes()));
    assertFalse(s.putIfAbsent("k", "b".getBytes()));
    assertEquals("a", new String(s.get("k").orElseThrow()));
    assertTrue(s.get("missing").isEmpty());
    s.put("dir/x", new byte[] {1});
    s.put("dir/y", new byte[] {2});
    assertEquals(List.of("dir/x", "dir/y"), s.list("dir/"));
  }

  @Test
  void concurrentWritersOnS3() throws Exception {
    S3ObjectStore s = store();
    Table.create(s, TestData.SCHEMA);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    List<Future<?>> fs = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final long base = w * 1000L;
      fs.add(pool.submit(() -> {
        Table t = Table.open(s);
        for (int c = 0; c < 5; c++) t.append(TestData.rows(base + c * 10, 10));
        return null;
      }));
    }
    for (var f : fs) f.get();
    pool.shutdown();
    Snapshot snap = Table.open(s).snapshot();
    assertEquals(20, snap.version());
    assertEquals(200, snap.numRecords());
  }
}
