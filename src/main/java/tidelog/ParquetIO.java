package tidelog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Reads and writes Parquet files entirely in memory, so data files can be
 * stored in any {@link tidelog.store.ObjectStore} without a Hadoop file system.
 */
public final class ParquetIO {
  private ParquetIO() {}

  /** A serialized data file and its per-column statistics. */
  public record Written(byte[] bytes, long numRecords, Map<String, ColumnRange> stats) {}

  private static final Configuration CONF = new Configuration(false);

  public static MessageType toParquet(Schema schema) {
    Types.MessageTypeBuilder b = Types.buildMessage();
    for (Field f : schema.fields()) {
      Type.Repetition rep = f.nullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
      switch (f.type()) {
        case LONG -> b.primitive(PrimitiveTypeName.INT64, rep).named(f.name());
        case DOUBLE -> b.primitive(PrimitiveTypeName.DOUBLE, rep).named(f.name());
        case BOOLEAN -> b.primitive(PrimitiveTypeName.BOOLEAN, rep).named(f.name());
        case STRING -> b.primitive(PrimitiveTypeName.BINARY, rep).as(LogicalTypeAnnotation.stringType()).named(f.name());
      }
    }
    return b.named("tidelog");
  }

  public static Written write(Schema schema, List<Row> rows) {
    MessageType type = toParquet(schema);
    SimpleGroupFactory groups = new SimpleGroupFactory(type);
    BytesOutputFile out = new BytesOutputFile();
    Map<String, ColumnRange> stats = new LinkedHashMap<>();
    try (ParquetWriter<Group> w = ExampleParquetWriter.builder(out)
        .withConf(CONF)
        .withType(type)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()) {
      for (Row row : rows) {
        Group g = groups.newGroup();
        for (int i = 0; i < schema.size(); i++) {
          Field f = schema.fields().get(i);
          Object v = row.get(i);
          stats.put(f.name(), ColumnRange.add(stats.get(f.name()), v));
          if (v == null) continue;
          switch (f.type()) {
            case LONG -> g.add(i, (Long) v);
            case DOUBLE -> g.add(i, (Double) v);
            case BOOLEAN -> g.add(i, (Boolean) v);
            case STRING -> g.add(i, (String) v);
          }
        }
        w.write(g);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new Written(out.toByteArray(), rows.size(), stats);
  }

  /**
   * Reads all rows, mapping file columns to the table schema by name. Columns
   * missing from the file read as null; INT32 and FLOAT widen to LONG and DOUBLE
   * (useful for files written by Spark).
   */
  public static List<Row> read(byte[] data, Schema schema) {
    List<Row> rows = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(new BytesInputFile(data))) {
      MessageType fileType = reader.getFooter().getFileMetaData().getSchema();
      int[] fileIndex = new int[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        String name = schema.fields().get(i).name();
        fileIndex[i] = fileType.containsField(name) ? fileType.getFieldIndex(name) : -1;
      }
      MessageColumnIO io = new ColumnIOFactory().getColumnIO(fileType);
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        RecordReader<Group> rr = io.getRecordReader(pages, new GroupRecordConverter(fileType));
        for (long r = 0; r < pages.getRowCount(); r++) {
          Group g = rr.read();
          Object[] values = new Object[schema.size()];
          for (int i = 0; i < schema.size(); i++) {
            int fi = fileIndex[i];
            if (fi < 0 || g.getFieldRepetitionCount(fi) == 0) continue;
            values[i] = schema.fields().get(i).type().coerce(readValue(g, fi, fileType.getType(fi)));
          }
          rows.add(new Row(values));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private static Object readValue(Group g, int i, Type t) {
    PrimitiveType p = t.asPrimitiveType();
    return switch (p.getPrimitiveTypeName()) {
      case INT64 -> g.getLong(i, 0);
      case INT32 -> (long) g.getInteger(i, 0);
      case DOUBLE -> g.getDouble(i, 0);
      case FLOAT -> (double) g.getFloat(i, 0);
      case BOOLEAN -> g.getBoolean(i, 0);
      case BINARY -> g.getString(i, 0);
      default -> throw new IllegalArgumentException("unsupported Parquet type " + p);
    };
  }

  /** Derives a Tidelog schema from a Parquet file's footer (used when importing files). */
  public static Schema inferSchema(byte[] data) {
    try (ParquetFileReader reader = ParquetFileReader.open(new BytesInputFile(data))) {
      MessageType t = reader.getFooter().getFileMetaData().getSchema();
      List<Field> fields = new ArrayList<>();
      for (Type col : t.getFields()) {
        if (!col.isPrimitive()) throw new IllegalArgumentException("nested column " + col.getName() + " not supported");
        FieldType ft = switch (col.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32, INT64 -> FieldType.LONG;
          case FLOAT, DOUBLE -> FieldType.DOUBLE;
          case BOOLEAN -> FieldType.BOOLEAN;
          case BINARY -> FieldType.STRING;
          default -> throw new IllegalArgumentException("unsupported column type for " + col.getName());
        };
        fields.add(new Field(col.getName(), ft, col.getRepetition() != Type.Repetition.REQUIRED));
      }
      return new Schema(fields);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Computes statistics by reading the file (works for files from any writer). */
  public static Map<String, ColumnRange> computeStats(Schema schema, List<Row> rows) {
    Map<String, ColumnRange> stats = new HashMap<>();
    for (Row row : rows)
      for (int i = 0; i < schema.size(); i++) {
        String name = schema.fields().get(i).name();
        stats.put(name, ColumnRange.add(stats.get(name), row.get(i)));
      }
    return stats;
  }

  // ---------------------------------------------------------------------------
  // In-memory OutputFile / InputFile adapters.

  static final class BytesOutputFile implements OutputFile {
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    byte[] toByteArray() {
      return buf.toByteArray();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return createOrOverwrite(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      buf.reset();
      return new DelegatingPositionOutputStream(buf) {
        @Override
        public long getPos() {
          return buf.size();
        }
      };
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  static final class BytesInputFile implements InputFile {
    private final byte[] data;

    BytesInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      SeekableBytes in = new SeekableBytes(data);
      return new DelegatingSeekableInputStream(in) {
        @Override
        public long getPos() {
          return in.position();
        }

        @Override
        public void seek(long newPos) {
          in.seek(newPos);
        }
      };
    }
  }

  private static final class SeekableBytes extends ByteArrayInputStream {
    SeekableBytes(byte[] b) {
      super(b);
    }

    long position() {
      return pos;
    }

    void seek(long p) {
      if (p < 0 || p > count) throw new IllegalArgumentException("seek out of range: " + p);
      pos = (int) p;
    }
  }
}
