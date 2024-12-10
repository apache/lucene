package org.apache.lucene.store.s3.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.lucene.store.s3.client.internal.Retries;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.util.NamedThreadFactory;

public final class Multipart {

  private Multipart() {
    // prevent instantiation
  }

  public static Builder s3(Client s3) {
    Preconditions.checkNotNull(s3);
    return new Builder(s3);
  }

  public static final class Builder {

    private final Client s3;
    private String bucket;
    public String key;
    public ExecutorService executor;
    public long timeoutMs = TimeUnit.HOURS.toMillis(1);
    public Function<? super Request, ? extends Request> transform = x -> x;
    public int partSize = 5 * 1024 * 1024;
    public Retries<Void> retries;

    Builder(Client s3) {
      this.s3 = s3;
      this.retries = s3.retries().withValueShouldRetry(values -> false);
    }

    public Builder2 bucket(String bucket) {
      Preconditions.checkNotNull(bucket, "bucket cannot be null");
      this.bucket = bucket;
      return new Builder2(this);
    }
  }

  public static final class Builder2 {

    private final Builder b;

    Builder2(Builder b) {
      this.b = b;
    }

    public Builder3 key(String key) {
      Preconditions.checkNotNull(key, "key cannot be null");
      b.key = key;
      return new Builder3(b);
    }
  }

  public static final class Builder3 {

    private final Builder b;

    Builder3(Builder b) {
      this.b = b;
    }

    public Builder3 executor(ExecutorService executor) {
      Preconditions.checkNotNull(executor, "executor cannot be null");
      b.executor = executor;
      return this;
    }

    public Builder3 partTimeout(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration > 0, "duration must be positive");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.timeoutMs = unit.toMillis(duration);
      return this;
    }

    public Builder3 partSize(int partSize) {
      Preconditions.checkArgument(partSize >= 5 * 1024 * 1024);
      b.partSize = partSize;
      return this;
    }

    public Builder3 partSizeMb(int partSizeMb) {
      return partSize(partSizeMb * 1024 * 1024);
    }

    public Builder3 maxAttemptsPerAction(int maxAttempts) {
      Preconditions.checkArgument(maxAttempts >= 1, "maxAttempts must be at least one");
      b.retries = b.retries.withMaxAttempts(maxAttempts);
      return this;
    }

    public Builder3 retryInitialInterval(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.retries = b.retries.withInitialIntervalMs(unit.toMillis(duration));
      return this;
    }

    public Builder3 retryBackoffFactor(double factor) {
      Preconditions.checkArgument(factor >= 0, "retryBackoffFactory cannot be negative");
      b.retries = b.retries.withBackoffFactor(factor);
      return this;
    }

    public Builder3 retryMaxInterval(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.retries = b.retries.withMaxIntervalMs(unit.toMillis(duration));
      return this;
    }

    /**
     * Sets the level of randomness applied to the next retry interval. The next calculated retry
     * interval is multiplied by {@code (1 - jitter * Math.random())}. A value of zero means no
     * jitter, 1 means max jitter.
     *
     * @param jitter level of randomness applied to the retry interval
     * @return this
     */
    public Builder3 retryJitter(double jitter) {
      Preconditions.checkArgument(jitter >= 0 && jitter <= 1, "jitter must be between 0 and 1");
      b.retries = b.retries.withJitter(jitter);
      return this;
    }

    public Builder3 transformCreateRequest(Function<? super Request, ? extends Request> transform) {
      Preconditions.checkNotNull(transform, "transform cannot be null");
      b.transform = transform;
      return this;
    }

    public void upload(byte[] bytes, int offset, int length) {
      Preconditions.checkNotNull(bytes, "bytes cannot be null");
      try (OutputStream out = outputStream()) {
        out.write(bytes, offset, length);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public void upload(byte[] bytes) {
      upload(bytes, 0, bytes.length);
    }

    public void upload(Path file) {
      Preconditions.checkNotNull(file, "file cannot be null");
      upload(() -> file);
    }

    public void upload(Callable<? extends Path> factory) {
      Preconditions.checkNotNull(factory, "factory cannot be null");
      try (MultipartOutputStream out = outputStream()) {
        Files.copy(factory.call(), out);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public MultipartOutputStream outputStream() {
      if (b.executor == null) {
        b.executor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("s3-multipart"));
      }
      return new MultipartOutputStream(
          b.s3, b.bucket, b.key, b.transform, b.executor, b.timeoutMs, b.retries, b.partSize);
    }
  }

  //    private static void copy(InputStream in, OutputStream out) throws IOException {
  //        byte[] buffer = new byte[8192];
  //        int n;
  //        while ((n = in.read(buffer)) != -1) {
  //            out.write(buffer, 0, n);
  //        }
  //    }
}
