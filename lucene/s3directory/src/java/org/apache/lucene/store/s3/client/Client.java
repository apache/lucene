package org.apache.lucene.store.s3.client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.store.s3.client.internal.Clock;
import org.apache.lucene.store.s3.client.internal.Environment;
import org.apache.lucene.store.s3.client.internal.ExceptionFactoryExtended;
import org.apache.lucene.store.s3.client.internal.Retries;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;

public final class Client {

  private final Clock clock;
  private final String serviceName;
  private final Optional<String> region;
  private final Credentials credentials;
  private final HttpClient httpClient;
  private final int connectTimeoutMs;
  private final int readTimeoutMs;
  private final ExceptionFactory exceptionFactory;
  private final BaseUrlFactory baseUrlFactory;
  private final Retries<ResponseInputStream> retries;

  private Client(
      Clock clock,
      String serviceName,
      Optional<String> region,
      Credentials credentials,
      HttpClient httpClient,
      int connectTimeoutMs,
      int readTimeoutMs,
      ExceptionFactory exceptionFactory,
      BaseUrlFactory baseUrlFactory,
      Retries<ResponseInputStream> retries) {
    this.clock = clock;
    this.serviceName = serviceName;
    this.region = region;
    this.credentials = credentials;
    this.httpClient = httpClient;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.exceptionFactory = exceptionFactory;
    this.baseUrlFactory = baseUrlFactory;
    this.retries = retries;
  }

  public static Builder service(String serviceName) {
    Preconditions.checkNotNull(serviceName);
    return new Builder(serviceName);
  }

  ///////////////////////////////////////////////////
  //
  // Convenience methods for a few common services
  // Use service(serviceName) method for the rest
  //
  ///////////////////////////////////////////////////

  public static Builder s3() {
    return service("s3");
  }

  public static Builder sqs() {
    return service("sqs");
  }

  public static Builder iam() {
    return service("iam");
  }

  public static Builder ec2() {
    return service("ec2");
  }

  public static Builder sns() {
    return service("sns");
  }

  public static Builder lambda() {
    return service("lambda");
  }

  ///////////////////////////////////////////////////

  String serviceName() {
    return serviceName;
  }

  public Optional<String> region() {
    return region;
  }

  Credentials credentials() {
    return credentials;
  }

  HttpClient httpClient() {
    return httpClient;
  }

  Clock clock() {
    return clock;
  }

  ExceptionFactory exceptionFactory() {
    return exceptionFactory;
  }

  BaseUrlFactory baseUrlFactory() {
    return baseUrlFactory;
  }

  int connectTimeoutMs() {
    return connectTimeoutMs;
  }

  int readTimeoutMs() {
    return readTimeoutMs;
  }

  Retries<ResponseInputStream> retries() {
    return retries;
  }

  public Request url(String url) {
    Preconditions.checkNotNull(url);
    return new Request(this, url);
  }

  /**
   * Specify the path (can include query starting with ? at end of final segment).
   *
   * @param segments that will be joined together with the '/' character
   * @return request
   */
  public Request path(String... segments) {
    Preconditions.checkNotNull(segments, "segments cannot be null");
    return new Request(this, null, segments);
  }

  public Request query(String name, String value) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(value, "value cannot be null");
    return path("").query(name, value);
  }

  public Request attributePrefix(String attributePrefix) {
    return path("").attributePrefix(attributePrefix);
  }

  public Request attribute(String name, String value) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(value, "value cannot be null");
    return path("").attribute(name, value);
  }

  public static final class Builder {

    // from
    // https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
    private static final Set<Integer> RETRY_STATUS_CODES =
        new HashSet<>( //
            Arrays.asList( //
                400, // BAD_REQUEST
                403, // FORBIDDEN
                408, // REQUEST_TIMEOUT
                429, // TOO_MANY_REQUESTS
                500, // INTERNAL_SERVER_ERROR
                502, // BAD_GATEWAY
                503, // SERVICE_UNAVAILABLE
                509 // BANDWIDTH_LIMIT_EXCEEDED
                ));

    private final String serviceName;
    private Optional<String> region = Optional.empty();
    private String accessKey;
    private Credentials credentials;
    private HttpClient httpClient = HttpClient.defaultClient();
    private int connectTimeoutMs = 30000;
    private int readTimeoutMs = 300000;
    private ExceptionFactory exceptionFactory = ExceptionFactory.DEFAULT;
    private Clock clock = Clock.DEFAULT;
    private Environment environment = Environment.instance();
    private BaseUrlFactory baseUrlFactory = BaseUrlFactory.DEFAULT;
    private Retries<ResponseInputStream> retries =
        Retries.create(
            ris -> RETRY_STATUS_CODES.contains(ris.statusCode()), //
            t -> t instanceof IOException || t instanceof UncheckedIOException);

    private Builder(String serviceName) {
      this.serviceName = serviceName;
    }

    // VisibleForTesting
    Builder environment(Environment environment) {
      Preconditions.checkNotNull(environment, "environment cannot be null");
      this.environment = environment;
      return this;
    }

    public Builder4 defaultClient() {
      return regionFromEnvironment().credentialsFromEnvironment();
    }

    public Builder4 from(Client client) {
      Preconditions.checkNotNull(client, "client cannot be null");
      this.region = client.region;
      this.credentials = client.credentials;
      this.httpClient = client.httpClient;
      this.connectTimeoutMs = client.connectTimeoutMs;
      this.readTimeoutMs = client.readTimeoutMs;
      this.exceptionFactory = client.exceptionFactory;
      return new Builder4(this);
    }

    public Builder2 regionFromEnvironment() {
      return region(environment.get("AWS_REGION"));
    }

    public Builder2 region(Optional<String> region) {
      Preconditions.checkNotNull(region, "region cannot be null");
      this.region = region;
      return new Builder2(this);
    }

    public Builder2 region(String region) {
      Preconditions.checkNotNull(region, "region cannot be null");
      return region(Optional.of(region));
    }

    public Builder2 regionNone() {
      return region(Optional.empty());
    }
  }

  public static final class Builder2 {
    private final Builder b;

    private Builder2(Builder b) {
      this.b = b;
    }

    public Builder4 credentialsFromEnvironment() {
      b.credentials = b.environment.credentials();
      return new Builder4(b);
    }

    public Builder4 credentialsFromSystemProperties() {
      return credentials(Credentials.fromSystemProperties());
    }

    public Builder3 accessKey(String accessKey) {
      Preconditions.checkNotNull(accessKey);
      b.accessKey = accessKey;
      return new Builder3(b);
    }

    public Builder4 credentials(Credentials credentials) {
      Preconditions.checkNotNull(credentials);
      b.credentials = credentials;
      return new Builder4(b);
    }
  }

  public static final class Builder3 {
    private final Builder b;

    private Builder3(Builder b) {
      this.b = b;
    }

    public Builder4 secretKey(String secretKey) {
      Preconditions.checkNotNull(secretKey);
      b.credentials = Credentials.of(b.accessKey, secretKey);
      return new Builder4(b);
    }
  }

  public static final class Builder4 {
    private final Builder b;

    private Builder4(Builder b) {
      this.b = b;
    }

    public Builder4 baseUrlFactory(BaseUrlFactory factory) {
      b.baseUrlFactory = factory;
      return this;
    }

    public Builder4 httpClient(HttpClient httpClient) {
      b.httpClient = httpClient;
      return this;
    }

    public Builder4 retryInitialInterval(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.retries = b.retries.withInitialIntervalMs(unit.toMillis(duration));
      return this;
    }

    public Builder4 retryMaxAttempts(int maxAttempts) {
      Preconditions.checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
      b.retries = b.retries.withMaxAttempts(maxAttempts);
      return this;
    }

    public Builder4 retryBackoffFactor(double factor) {
      Preconditions.checkArgument(factor >= 0, "backoffFactor cannot be negative");
      b.retries = b.retries.withBackoffFactor(factor);
      return this;
    }

    public Builder4 retryMaxInterval(long duration, TimeUnit unit) {
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
    public Builder4 retryJitter(double jitter) {
      Preconditions.checkArgument(jitter >= 0 && jitter <= 1, "jitter must be between 0 and 1");
      b.retries = b.retries.withJitter(jitter);
      return this;
    }

    public Builder4 retryCondition(Predicate<? super ResponseInputStream> shouldRetry) {
      Preconditions.checkNotNull(shouldRetry, "shouldRetry cannot be null");
      b.retries = b.retries.withValueShouldRetry(shouldRetry);
      return this;
    }

    public Builder4 retryStatusCodes(Integer... statusCodes) {
      return retryStatusCodes(Arrays.asList(statusCodes));
    }

    public Builder4 retryStatusCodes(Collection<Integer> statusCodes) {
      Preconditions.checkNotNull(statusCodes, "statusCodes cannot be null");
      Set<Integer> set = new HashSet<>(statusCodes);
      return retryCondition(ris -> set.contains(ris.statusCode()));
    }

    /**
     * Default behaviour is to retry IOException and UncheckedIOException.
     *
     * @param shouldRetry returns true if should retry
     * @return this
     */
    public Builder4 retryException(Predicate<? super Throwable> shouldRetry) {
      Preconditions.checkNotNull(shouldRetry, "shouldRetry cannot be null");
      b.retries = b.retries.withThrowableShouldRetry(shouldRetry);
      return this;
    }

    public Builder4 connectTimeout(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.connectTimeoutMs = (int) unit.toMillis(duration);
      return this;
    }

    public Builder4 readTimeout(long duration, TimeUnit unit) {
      Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
      Preconditions.checkNotNull(unit, "unit cannot be null");
      b.readTimeoutMs = (int) unit.toMillis(duration);
      return this;
    }

    public Builder4 exceptionFactory(ExceptionFactory exceptionFactory) {
      b.exceptionFactory = exceptionFactory;
      return this;
    }

    public Builder4 exception(
        Predicate<? super Response> predicate,
        Function<? super Response, ? extends RuntimeException> factory) {
      b.exceptionFactory = new ExceptionFactoryExtended(b.exceptionFactory, predicate, factory);
      return this;
    }

    public Builder4 clock(Clock clock) {
      b.clock = clock;
      return this;
    }

    public Client build() {
      return new Client(
          b.clock,
          b.serviceName,
          b.region,
          b.credentials,
          b.httpClient,
          b.connectTimeoutMs,
          b.readTimeoutMs,
          b.exceptionFactory,
          b.baseUrlFactory,
          b.retries);
    }
  }
}
