package org.apache.lucene.store.s3.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ResponseInputStream extends InputStream {

  private final Closeable closeable; // nullable
  private final int statusCode;
  private final Map<String, List<String>> headers;
  private final InputStream content;

  public ResponseInputStream(
      HttpURLConnection connection,
      int statusCode,
      Map<String, List<String>> headers,
      InputStream content) {
    this(() -> connection.disconnect(), statusCode, headers, content);
  }

  public ResponseInputStream(
      Closeable closeable, int statusCode, Map<String, List<String>> headers, InputStream content) {
    this.closeable = closeable;
    this.statusCode = statusCode;
    this.headers = headers;
    this.content = content;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return content.read(b, off, len);
  }

  @Override
  public int read() throws IOException {
    return content.read();
  }

  @Override
  public void close() throws IOException {
    try {
      content.close();
    } finally {
      closeable.close();
    }
  }

  public int statusCode() {
    return statusCode;
  }

  public Map<String, List<String>> headers() {
    return headers;
  }

  public Optional<String> header(String name) {
    for (String key : headers.keySet()) {
      if (name.equalsIgnoreCase(key)) {
        return Optional.of(headers.get(key).stream().collect(Collectors.joining(",")));
      }
    }
    return Optional.empty();
  }
}
