package tidelog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import tidelog.Actions.Action;

/** JSON-lines encoding of log entries. */
final class Json {
  static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private Json() {}

  static byte[] encodeLines(List<Action> actions) {
    StringBuilder sb = new StringBuilder();
    try {
      for (Action a : actions) sb.append(MAPPER.writeValueAsString(a)).append('\n');
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  static List<Action> decodeLines(byte[] data) {
    List<Action> out = new ArrayList<>();
    try {
      for (String line : new String(data, StandardCharsets.UTF_8).split("\n")) {
        if (!line.isBlank()) out.add(MAPPER.readValue(line, Action.class));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }

  static byte[] encode(Object o) {
    try {
      return MAPPER.writeValueAsBytes(o);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static <T> T decode(byte[] data, Class<T> type) {
    try {
      return MAPPER.readValue(data, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
