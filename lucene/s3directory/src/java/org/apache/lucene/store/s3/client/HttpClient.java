package org.apache.lucene.store.s3.client;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.apache.lucene.store.s3.client.internal.HttpClientDefault;

public interface HttpClient {

  ResponseInputStream request(
      URL endpointUrl,
      String httpMethod,
      Map<String, String> headers,
      byte[] requestBody,
      int connectTimeoutMs,
      int readTimeoutMs)
      throws IOException;

  static HttpClient defaultClient() {
    return HttpClientDefault.INSTANCE;
  }
}
