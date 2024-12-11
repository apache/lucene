package org.apache.lucene.store.s3.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.s3.client.HttpClient;
import org.apache.lucene.store.s3.client.ResponseInputStream;
import org.apache.lucene.store.s3.client.internal.util.Util;

public final class HttpClientDefault implements HttpClient {

  public static final HttpClientDefault INSTANCE = new HttpClientDefault();

  private HttpClientDefault() {}

  @Override
  public ResponseInputStream request(
      URL endpointUrl,
      String httpMethod,
      Map<String, String> headers,
      byte[] requestBody,
      int connectTimeoutMs,
      int readTimeoutMs)
      throws IOException {
    HttpURLConnection connection =
        Util.createHttpConnection(
            endpointUrl, httpMethod, headers, connectTimeoutMs, readTimeoutMs);
    return request(connection, requestBody);
  }

  // VisibleForTesting
  static ResponseInputStream request(HttpURLConnection connection, byte[] requestBody) {
    int responseCode;
    Map<String, List<String>> responseHeaders;
    InputStream is;
    try {
      if (requestBody != null) {
        OutputStream out = connection.getOutputStream();
        out.write(requestBody);
        out.flush();
      }
      responseHeaders = connection.getHeaderFields();
      responseCode = connection.getResponseCode();
      if (isOk(responseCode)) {
        is = connection.getInputStream();
      } else {
        is = connection.getErrorStream();
      }
      if (is == null) {
        is = Util.emptyInputStream();
      }
    } catch (IOException e) {
      try {
        connection.disconnect();
      } catch (Throwable e2) {
        // ignore
        e2.getCause();
      }
      throw new UncheckedIOException(e);
    }
    return new ResponseInputStream(connection, responseCode, responseHeaders, is);
  }

  private static boolean isOk(int responseCode) {
    return responseCode >= 200 && responseCode <= 299;
  }
}
