package tidelog.store;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Object store on a local (POSIX) file system. Writes go to a temporary file
 * first so readers never see a partially written object; putIfAbsent publishes
 * with a hard link, which fails atomically if the target already exists.
 */
public final class LocalObjectStore implements ObjectStore {
  private final Path root;

  public LocalObjectStore(Path root) {
    this.root = root.toAbsolutePath();
  }

  private Path resolve(String key) {
    return root.resolve(key);
  }

  private Path writeTemp(Path target, byte[] data) throws IOException {
    Files.createDirectories(target.getParent());
    Path tmp = target.getParent().resolve("." + target.getFileName() + "." + UUID.randomUUID() + ".tmp");
    Files.write(tmp, data);
    return tmp;
  }

  @Override
  public Optional<byte[]> get(String key) throws IOException {
    try {
      return Optional.of(Files.readAllBytes(resolve(key)));
    } catch (NoSuchFileException e) {
      return Optional.empty();
    }
  }

  @Override
  public void put(String key, byte[] data) throws IOException {
    Path target = resolve(key);
    Path tmp = writeTemp(target, data);
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public boolean putIfAbsent(String key, byte[] data) throws IOException {
    Path target = resolve(key);
    Path tmp = writeTemp(target, data);
    try {
      Files.createLink(target, tmp);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  @Override
  public List<String> list(String prefix) throws IOException {
    // Walk the directory that contains the prefix and filter by full key.
    // Other writers create and delete temporary files concurrently, so files
    // that vanish during the walk are ignored.
    String dir = prefix.contains("/") ? prefix.substring(0, prefix.lastIndexOf('/')) : "";
    Path start = dir.isEmpty() ? root : resolve(dir);
    List<String> keys = new ArrayList<>();
    if (!Files.isDirectory(start)) return keys;
    Files.walkFileTree(start, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        String name = file.getFileName().toString();
        if (attrs.isRegularFile() && !name.startsWith(".")) {
          String key = root.relativize(file).toString().replace('\\', '/');
          if (key.startsWith(prefix)) keys.add(key);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
        if (e instanceof NoSuchFileException) return FileVisitResult.CONTINUE;
        throw e;
      }
    });
    keys.sort(null);
    return keys;
  }

  @Override
  public void delete(String key) throws IOException {
    Files.deleteIfExists(resolve(key));
  }
}
