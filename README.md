# Tidelog

Tidelog is an ACID table format for object storage, written in Java. A table is a set of
Parquet data files plus an ordered **transaction log**. Every change (append, delete,
compaction) becomes visible atomically as one new log entry, readers always see a
consistent snapshot, and old versions stay queryable (time travel).

It runs against the local file system, any S3-compatible store (AWS S3, MinIO), or memory.
No database, lock service, or Hadoop cluster is needed. The only coordination primitive is a
conditional "create if absent" write.

## Layout

```
<table>/
  _tidelog/
    00000000000000000000.json             # version 0: table metadata (schema)
    00000000000000000001.json             # version 1: add data files
    ...
    00000000000000000010.checkpoint.json  # full table state at version 10
    _last_checkpoint                      # hint: latest checkpoint version
  data/
    part-<uuid>.parquet
```

Each commit file is JSON lines with one action per line:

```json
{"commitInfo":{"timestamp":1793278213552,"operation":"DELETE","readVersion":4,"isBlindAppend":false,"metrics":{"rowsDeleted":25}}}
{"remove":{"path":"data/part-1c...parquet","deletionTimestamp":1793278213550}}
{"add":{"path":"data/part-9f...parquet","size":2310,"numRecords":75,"modificationTime":1793278213549,
        "stats":{"id":{"min":0,"max":99,"nullCount":0},"region":{"min":"NORTH","max":"WEST","nullCount":0}}}}
```

The table at version N is the replay of versions 0..N. A checkpoint holds the full state, so
readers start from the newest checkpoint at or below N and replay only the commits after it.

## How commits work (optimistic concurrency control)

1. A transaction reads a snapshot (version `v`) and writes its new Parquet files. Data files
   have unique names, so writers never collide on them.
2. It commits by creating `_tidelog/<v+1>.json` with a **conditional write**:
   - `If-None-Match: *` on S3/MinIO
   - a hard link on the local file system
   - `putIfAbsent` in memory

   Exactly one writer can create a given version.
3. If another writer already took `v+1`, the transaction reads the winning commits and checks
   for conflicts. If there are none, it retries at the next version.

| This transaction | Conflicts with a concurrent commit that... |
|---|---|
| blind append | never conflicts |
| delete with a predicate (copy-on-write) | removed any file it rewrote, or added a file whose min/max stats could match its predicate |
| compaction | removed any file it is compacting (concurrent appends are fine) |
| any | changed the table metadata |

This gives snapshot isolation for readers. Deletes are serializable with respect to concurrent
appends of matching data.

## Data skipping and clustering

Every `add` action records per-column min/max/null-count statistics. A scan with a filter skips
any file whose statistics prove it cannot match.

`compact(targetRows, clusterBy)` does two jobs:
- It rewrites many small files into fewer large ones.
- With `clusterBy`, it rewrites the table sorted by a column. Each file then covers a narrow
  key range, so range filters touch only a few files.

## Usage

```bash
mvn package                      # builds target/tidelog.jar and runs the tests
alias tidelog='java -jar target/tidelog.jar'

tidelog create   --table file:///tmp/events --schema "id:long,region:string,amount:double,flag:boolean?"
tidelog append   --table file:///tmp/events --csv events.csv
tidelog scan     --table file:///tmp/events --where "id BETWEEN 100 AND 200 AND region = 'EAST'"
tidelog delete   --table file:///tmp/events --where "region = 'WEST'"
tidelog compact  --table file:///tmp/events --target-rows 100000 --cluster-by id
tidelog scan     --table file:///tmp/events --version 1          # time travel
tidelog history  --table file:///tmp/events
```

S3 / MinIO:

```bash
docker compose up -d   # local MinIO
export TIDELOG_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
tidelog create --table s3://my-bucket/tables/events --schema "id:long,region:string"
```

Java API:

```java
Table t = Table.create(ObjectStore.open("file:///tmp/events"), Schema.parse("id:long,region:string"));
t.append(List.of(new Row(1L, "EAST"), new Row(2L, "WEST")));
Transaction tx = t.newTransaction("DELETE");
tx.delete(Filter.parse("region = 'WEST'"));
tx.commit();                                  // throws ConflictException on a real conflict
Snapshot v1 = t.snapshotAt(1);                // time travel
ScanResult r = t.snapshot().scan(Filter.parse("id >= 1"));
```

### Ingesting PySpark output

[`examples/pyspark_ingest.py`](examples/pyspark_ingest.py) runs a full ingest:

1. A PySpark job writes 2M events as 16 Parquet files.
2. Tidelog imports all 16 files in one atomic commit.
3. A range query runs, then the table is clustered, then the same query runs again.

```
spark wrote 16 parquet files
imported 16 files in version 1
199921 rows (version 1; files scanned 16 of 16, skipped 0)
version 2: replaced 16 files with 16
199921 rows (version 2; files scanned 3 of 16, skipped 13)
```

## Results

Measured on a 2-vCPU Linux VM (`java -cp target/tidelog.jar tidelog.bench.Benchmark`):

| Scenario | Result |
|---|---|
| 16 writers x 25 appends, one table, local FS | 400 versions committed, 40,000 rows, **0 lost commits**, about 6.4 attempts per commit under contention |
| 50 range queries (1% of keys each), 200 files x 1,000 rows of unsorted data | **98.5% fewer files scanned** after clustering (200 to 3 per query) |
| 2M rows from PySpark, 10% range query | files scanned 16 to 3 (**81% fewer**) after clustering |

## Tests

`mvn test` runs 21 tests covering:
- Parquet round trips with nulls
- copy-on-write deletes
- time travel by version and timestamp
- checkpoint vs full-replay equivalence at every version
- each conflict rule
- compaction racing with appends and with deletes
- data skipping correctness
- 16 concurrent writers (both in memory and on the local file system), checking every row lands exactly once

`S3ObjectStoreTest` adds S3 conditional writes and concurrent writers. It runs when
`TIDELOG_S3_ENDPOINT` is set (MinIO via `docker compose`, or another S3 endpoint such as the
moto emulator).

## Limitations

- Flat schemas only (long, double, string, boolean); no schema evolution yet.
- Deletes are copy-on-write; there are no deletion vectors.
- Removed files are kept for time travel; there is no `vacuum` command yet.
- Data skipping uses file-level min/max statistics, not Parquet page indexes.
