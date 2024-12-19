package org.apache.lucene.store.s3.client;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;

public final class Response {

  private final Map<String, List<String>> headers;
  private final byte[] content;
  private final int statusCode;

  public Response(Map<String, List<String>> headers, byte[] content, int statusCode) {
    this.headers = headers;
    this.content = content;
    this.statusCode = statusCode;
  }

  public Map<String, List<String>> headers() {
    return headers;
  }

  public Optional<String> firstHeader(String name) {
    List<String> h = headers.get(name);
    if (h == null || h.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(h.get(0));
    }
  }

  public Optional<Instant> firstHeaderFullDate(String name) {
    return firstHeader(name) //
        .map(x -> ZonedDateTime.parse(x, Formats.FULL_DATE).toInstant());
  }

  /**
   * Returns those headers that start with {@code x-amz-meta-} (and removes that prefix).
   *
   * @return headers that start with {@code x-amz-meta-} (and removes that prefix)
   */
  public Metadata metadata() {
    return new Metadata(
        headers //
            .entrySet() //
            .stream() //
            .filter(x -> x.getKey() != null) //
            .filter(x -> x.getKey().startsWith("x-amz-meta-")) //
            .collect(
                Collectors.toMap( //
                    x -> x.getKey().substring(11), //
                    x -> x.getValue().get(0))));
  }

  public Optional<String> metadata(String name) {
    Preconditions.checkNotNull(name);
    return metadata().value(name);
  }

  public byte[] content() {
    return content;
  }

  public String contentUtf8() {
    return new String(content, StandardCharsets.UTF_8);
  }

  public int statusCode() {
    return statusCode;
  }

  public boolean isOk() {
    return statusCode >= 200 && statusCode <= 299;
  }

  /**
   * Returns true if and only if status code is 2xx. Returns false if status code is 404 (NOT_FOUND)
   * and throws a {@link ServiceException} otherwise.
   *
   * @return true if status code 2xx, false if 404 otherwise throws ServiceException
   * @throws ServiceException if status code other than 2xx or 404
   */
  public boolean exists() {
    if (statusCode >= 200 && statusCode <= 299) {
      return true;
    } else if (statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
      return false;
    } else {
      throw new ServiceException(statusCode, "call failed");
    }
  }

  // TODO add toString method
}
