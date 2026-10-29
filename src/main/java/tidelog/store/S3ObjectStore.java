package tidelog.store;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Object store on S3-compatible storage (AWS S3, MinIO). Commits rely on
 * conditional writes ({@code If-None-Match: *}), which S3 and MinIO both
 * support, so no external lock service is needed.
 *
 * <p>Configuration for MinIO: set {@code TIDELOG_S3_ENDPOINT} (for example
 * {@code http://localhost:9000}) plus {@code AWS_ACCESS_KEY_ID} and
 * {@code AWS_SECRET_ACCESS_KEY}.
 */
public final class S3ObjectStore implements ObjectStore {
  private final S3Client s3;
  private final String bucket;
  private final String prefix;

  public S3ObjectStore(S3Client s3, String bucket, String prefix) {
    this.s3 = s3;
    this.bucket = bucket;
    this.prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
  }

  /** Builds a store for {@code s3://bucket/prefix}. */
  public static S3ObjectStore fromUri(String uri) {
    String rest = uri.substring("s3://".length());
    int slash = rest.indexOf('/');
    String bucket = slash < 0 ? rest : rest.substring(0, slash);
    String prefix = slash < 0 ? "" : rest.substring(slash + 1);
    return new S3ObjectStore(clientFromEnv(), bucket, prefix);
  }

  public static S3Client clientFromEnv() {
    var builder = S3Client.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .region(Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1")));
    String endpoint = System.getenv("TIDELOG_S3_ENDPOINT");
    if (endpoint != null && !endpoint.isEmpty()) {
      builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
    }
    String key = System.getenv("AWS_ACCESS_KEY_ID");
    String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (key != null && secret != null) {
      builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(key, secret)));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
    }
    return builder.build();
  }

  private String full(String key) {
    return prefix + key;
  }

  @Override
  public Optional<byte[]> get(String key) throws IOException {
    try {
      return Optional.of(s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(full(key)).build())
          .asByteArray());
    } catch (NoSuchKeyException e) {
      return Optional.empty();
    } catch (S3Exception e) {
      if (e.statusCode() == 404) return Optional.empty();
      throw new IOException(e);
    }
  }

  @Override
  public void put(String key, byte[] data) throws IOException {
    try {
      s3.putObject(PutObjectRequest.builder().bucket(bucket).key(full(key)).build(), RequestBody.fromBytes(data));
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, byte[] data) throws IOException {
    try {
      s3.putObject(PutObjectRequest.builder().bucket(bucket).key(full(key)).ifNoneMatch("*").build(),
          RequestBody.fromBytes(data));
      return true;
    } catch (S3Exception e) {
      // 412: the object exists. 409: a concurrent conditional write is in flight.
      if (e.statusCode() == 412 || e.statusCode() == 409) return false;
      throw new IOException(e);
    }
  }

  @Override
  public List<String> list(String keyPrefix) throws IOException {
    List<String> keys = new ArrayList<>();
    String token = null;
    try {
      do {
        var req = ListObjectsV2Request.builder().bucket(bucket).prefix(full(keyPrefix));
        if (token != null) req.continuationToken(token);
        ListObjectsV2Response resp = s3.listObjectsV2(req.build());
        for (S3Object o : resp.contents()) keys.add(o.key().substring(prefix.length()));
        token = resp.isTruncated() ? resp.nextContinuationToken() : null;
      } while (token != null);
    } catch (S3Exception e) {
      throw new IOException(e);
    }
    keys.sort(null);
    return keys;
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(full(key)).build());
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }
}
