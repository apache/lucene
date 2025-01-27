package org.apache.lucene.store.s3.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.store.s3.client.internal.Retries;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.store.s3.client.internal.util.Util;
import org.apache.lucene.store.s3.client.xml.XmlElement;

public final class Request {

  private final Client client;
  private Optional<String> region;
  private String url;
  private HttpMethod method = HttpMethod.GET;
  private final Map<String, List<String>> headers = new HashMap<>();
  private byte[] requestBody;
  private int connectTimeoutMs;
  private int readTimeoutMs;
  private int attributeNumber = 1;
  private Retries<ResponseInputStream> retries;
  private String attributePrefix = "Attribute";
  private String[] pathSegments;
  private final List<NameValue> queries = new ArrayList<>();
  private boolean signPayload = true;

  Request(Client client, String url, String... pathSegments) {
    this.client = client;
    this.url = url;
    this.pathSegments = pathSegments;
    this.region = client.region();
    this.connectTimeoutMs = client.connectTimeoutMs();
    this.readTimeoutMs = client.readTimeoutMs();
    this.retries = client.retries().copy();
  }

  public Request method(HttpMethod method) {
    Preconditions.checkNotNull(method);
    this.method = method;
    return this;
  }

  public Request query(String name, String value) {
    Preconditions.checkNotNull(name);
    queries.add(new NameValue(name, value));
    return this;
  }

  public Request query(String name) {
    return query(name, null);
  }

  public Request attributePrefix(String attributePrefix) {
    this.attributePrefix = attributePrefix;
    this.attributeNumber = 1;
    return this;
  }

  public Request attribute(String name, String value) {
    int i = attributeNumber;
    attributeNumber++;
    return query(attributePrefix + "." + i + ".Name", name) //
        .query(attributePrefix + "." + i + ".Value", value);
  }

  public Request header(String name, String value) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(value);
    RequestHelper.put(headers, name, value);
    return this;
  }

  public Request signPayload(boolean signPayload) {
    this.signPayload = signPayload;
    return this;
  }

  public Request unsignedPayload() {
    return signPayload(false);
  }

  /**
   * Adds the header {@code x-amz-meta-KEY:value}. {@code KEY} is obtained from {@code key} by
   * converting to lower-case (headers are case-insensitive) and only retaining alphabetical and
   * digit characters.
   *
   * @param key metadata key
   * @param value metadata value
   * @return request builder
   */
  public Request metadata(String key, String value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    return header("x-amz-meta-" + Util.canonicalMetadataKey(key), value);
  }

  public Request requestBody(byte[] requestBody) {
    Preconditions.checkNotNull(requestBody);
    this.requestBody = requestBody;
    return this;
  }

  public Request requestBody(String requestBody) {
    Preconditions.checkNotNull(requestBody);
    return requestBody(requestBody.getBytes(StandardCharsets.UTF_8));
  }

  public Request region(String region) {
    Preconditions.checkNotNull(region);
    this.region = Optional.of(region);
    return this;
  }

  public Request connectTimeout(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
    Preconditions.checkNotNull(unit, "unit cannot be null");
    this.connectTimeoutMs = (int) unit.toMillis(duration);
    return this;
  }

  public Request readTimeout(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
    Preconditions.checkNotNull(unit, "unit cannot be null");
    this.readTimeoutMs = (int) unit.toMillis(duration);
    return this;
  }

  public Request retryInitialInterval(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
    Preconditions.checkNotNull(unit, "unit cannot be null");
    retries = retries.withInitialIntervalMs(unit.toMillis(duration));
    return this;
  }

  public Request retryMaxAttempts(int maxAttempts) {
    Preconditions.checkArgument(maxAttempts >= 0, "retryMaxAttempts cannot be negative");
    retries = retries.withMaxAttempts(maxAttempts);
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
  public Request retryJitter(double jitter) {
    Preconditions.checkArgument(jitter >= 0 && jitter <= 1, "jitter must be between 0 and 1");
    retries = retries.withJitter(jitter);
    return this;
  }

  public Request retryBackoffFactor(double factor) {
    Preconditions.checkArgument(factor >= 0, "retryBackoffFactor cannot be negative");
    retries = retries.withBackoffFactor(factor);
    return this;
  }

  public Request retryMaxInterval(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
    Preconditions.checkNotNull(unit, "unit cannot be null");
    retries = retries.withMaxIntervalMs(unit.toMillis(duration));
    return this;
  }

  /**
   * Opens a connection and makes the request. This method returns all the response information
   * including headers, status code, request body as an InputStream. If an error status code is
   * encountered (outside 200-299) then an exception is <b>not</b> thrown (unlike the other methods
   * .response*). The caller <b>must close</b> the InputStream when finished with it.
   *
   * @return all response information, the caller must close the InputStream when finished with it
   */
  public ResponseInputStream responseInputStream() {
    String u =
        calculateUrl(
            url,
            client.serviceName(),
            region,
            queries,
            Arrays.asList(pathSegments),
            client.baseUrlFactory());
    return retries //
        .call(
        () ->
            RequestHelper.request(
                client.clock(),
                client.httpClient(),
                u,
                method,
                RequestHelper.combineHeaders(headers),
                requestBody,
                client.serviceName(),
                region,
                client.credentials(),
                connectTimeoutMs,
                readTimeoutMs,
                signPayload));
  }

  /**
   * Opens a connection and makes the request. This method returns all the response information
   * including headers, status code, request body as a byte array. If an error status code is
   * encountered (outside 200-299) then an exception is <b>not</b> thrown (unlike the other methods
   * .response*).
   *
   * @return all response information
   */
  public Response response() {
    ResponseInputStream r = responseInputStream();
    final byte[] bytes;
    if (hasBody(r)) {
      bytes = Util.readBytesAndClose(r);
    } else {
      bytes = new byte[0];
    }
    return new Response(r.headers(), bytes, r.statusCode());
  }

  /**
   * Opens a connection and makes the request. This method returns all the response information
   * including headers, status code, request body as a byte array. If the expected status code is
   * not encountered then a {@link ServiceException} is thrown.
   *
   * @return all response information
   * @throws ServiceException exception
   */
  public Response responseExpectStatusCode(int expectedStatusCode) {
    Response r = response();
    if (r.statusCode() == expectedStatusCode) {
      return r;
    } else {
      throw new ServiceException(r.statusCode(), r.contentUtf8());
    }
  }

  // VisibleForTesting
  static boolean hasBody(ResponseInputStream r) {
    return r.header("Content-Length").isPresent()
        || r.header("Transfer-Encoding").orElse("").equalsIgnoreCase("chunked");
  }

  private static String calculateUrl(
      String url,
      String serviceName,
      Optional<String> region,
      List<NameValue> queries,
      List<String> pathSegments,
      BaseUrlFactory baseUrlFactory) {
    String u = url;
    if (u == null) {
      String baseUrl = baseUrlFactory.create(serviceName, region);
      Preconditions.checkNotNull(baseUrl, "baseUrl cannot be null");
      u =
          trimAndEnsureHasTrailingSlash(baseUrl) //
              + pathSegments //
                  .stream() //
                  .map(x -> trimAndRemoveLeadingAndTrailingSlashes(x)) //
                  .collect(Collectors.joining("/"));
    }
    // add queries
    for (NameValue nv : queries) {
      if (!u.contains("?")) {
        u += "?";
      }
      if (!u.endsWith("?")) {
        u += "&";
      }
      if (nv.value != null) {
        u += Util.urlEncode(nv.name, false) + "=" + Util.urlEncode(nv.value, false);
      } else {
        u += Util.urlEncode(nv.name, false);
      }
    }
    return u;
  }

  // VisibleForTesting
  static String trimAndEnsureHasTrailingSlash(String s) {
    String r = s.trim();
    if (r.endsWith("/")) {
      return r;
    } else {
      return r + "/";
    }
  }

  public byte[] responseAsBytes() {
    Response r = response();
    Optional<? extends RuntimeException> exception = client.exceptionFactory().create(r);
    if (!exception.isPresent()) {
      return r.content();
    } else {
      throw exception.get();
    }
  }

  /**
   * Returns true if and only if status code is 2xx. Returns false if status code is 404 (NOT_FOUND)
   * and throws a {@link ServiceException} otherwise.
   *
   * @return true if status code 2xx, false if 404 otherwise throws ServiceException
   * @throws ServiceException if status code other than 2xx or 404
   */
  public boolean exists() {
    return response().exists();
  }

  public void execute() {
    responseAsBytes();
  }

  public String responseAsUtf8() {
    return new String(responseAsBytes(), StandardCharsets.UTF_8);
  }

  public XmlElement responseAsXml() {
    return XmlElement.parse(responseAsUtf8());
  }

  public String presignedUrl(long expiryDuration, TimeUnit unit) {
    String u =
        calculateUrl(
            url,
            client.serviceName(),
            region,
            queries,
            Arrays.asList(pathSegments),
            client.baseUrlFactory());
    return RequestHelper.presignedUrl(
        client.clock(),
        u,
        method.toString(),
        RequestHelper.combineHeaders(headers),
        requestBody,
        client.serviceName(),
        region,
        client.credentials(),
        connectTimeoutMs,
        readTimeoutMs,
        unit.toSeconds(expiryDuration),
        signPayload);
  }

  // VisibleForTesting
  static String trimAndRemoveLeadingAndTrailingSlashes(String s) {
    Preconditions.checkNotNull(s);
    s = s.trim();
    if (s.startsWith("/")) {
      s = s.substring(1);
    }
    if (s.endsWith("/")) {
      s = s.substring(0, s.length() - 1);
    }
    return s;
  }

  private static final class NameValue {
    final String name;
    final String value;

    NameValue(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }
}
