package org.apache.lucene.store.s3.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.store.s3.client.internal.Clock;
import org.apache.lucene.store.s3.client.internal.auth.AwsSignatureVersion4;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.store.s3.client.internal.util.Util;

final class RequestHelper {

  private RequestHelper() {
    // prevent instantiation
  }

  static void put(Map<String, List<String>> map, String name, String value) {
    Preconditions.checkNotNull(map);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(value);
    List<String> list = map.get(name);
    if (list == null) {
      list = new ArrayList<>();
      map.put(name, list);
    }
    list.add(value);
  }

  static Map<String, String> combineHeaders(Map<String, List<String>> headers) {
    Preconditions.checkNotNull(headers);
    return headers.entrySet().stream()
        .collect(
            Collectors.toMap(
                x -> x.getKey(), x -> x.getValue().stream().collect(Collectors.joining(","))));
  }

  static String presignedUrl(
      Clock clock,
      String url,
      String method,
      Map<String, String> headers,
      byte[] requestBody,
      String serviceName,
      Optional<String> regionName,
      Credentials credentials,
      int connectTimeoutMs,
      int readTimeoutMs,
      long expirySeconds,
      boolean signPayload) {

    // the region-specific endpoint to the target object expressed in path style
    URL endpointUrl = Util.toUrl(url);

    Map<String, String> h = new HashMap<>(headers);
    final String contentHashString;
    if (isEmpty(requestBody)) {
      contentHashString = AwsSignatureVersion4.UNSIGNED_PAYLOAD;
      h.put("x-amz-content-sha256", "");
    } else if (!signPayload) {
      contentHashString = AwsSignatureVersion4.UNSIGNED_PAYLOAD;
      h.put("x-amz-content-sha256", contentHashString);
    } else {
      // compute hash of the body content
      byte[] contentHash = Util.sha256(requestBody);
      contentHashString = Util.toHex(contentHash);
      h.put("content-length", "" + requestBody.length);
      h.put("x-amz-content-sha256", contentHashString);
    }

    List<Parameter> parameters = extractQueryParameters(endpointUrl);
    // don't use Collectors.toMap because it doesn't accept null values in map
    Map<String, String> q = new HashMap<>();
    parameters.forEach(p -> q.put(p.name, p.value));

    // construct the query parameter string to accompany the url

    // for SignatureV4, the max expiry for a presigned url is 7 days,
    // expressed in seconds
    q.put("X-Amz-Expires", "" + expirySeconds);

    String authorizationQueryParameters =
        AwsSignatureVersion4.computeSignatureForQueryAuth(
            endpointUrl,
            method,
            serviceName,
            regionName,
            clock,
            h,
            q,
            contentHashString,
            credentials.accessKey(),
            credentials.secretKey(),
            credentials.sessionToken());

    // build the presigned url to incorporate the authorization elements as query
    // parameters
    String u = endpointUrl.toString();
    final String presignedUrl;
    if (u.contains("?")) {
      presignedUrl = u + "&" + authorizationQueryParameters;
    } else {
      presignedUrl = u + "?" + authorizationQueryParameters;
    }
    return presignedUrl;
  }

  private static void includeTokenIfPresent(Credentials credentials, Map<String, String> h) {
    if (credentials.sessionToken().isPresent()) {
      h.put("x-amz-security-token", credentials.sessionToken().get());
    }
  }

  static ResponseInputStream request(
      Clock clock,
      HttpClient httpClient,
      String url,
      HttpMethod method,
      Map<String, String> headers,
      byte[] requestBody,
      String serviceName,
      Optional<String> regionName,
      Credentials credentials,
      int connectTimeoutMs,
      int readTimeoutMs, //
      boolean signPayload)
      throws IOException {

    // the region-specific endpoint to the target object expressed in path style
    URL endpointUrl = Util.toUrl(url);

    Map<String, String> h = new HashMap<>(headers);
    final String contentHashString;
    if (isEmpty(requestBody)) {
      contentHashString = AwsSignatureVersion4.EMPTY_BODY_SHA256;
    } else {
      if (!signPayload) {
        contentHashString = AwsSignatureVersion4.UNSIGNED_PAYLOAD;
      } else {
        // compute hash of the body content
        byte[] contentHash = Util.sha256(requestBody);
        contentHashString = Util.toHex(contentHash);
      }
      h.put("content-length", "" + requestBody.length);
    }
    h.put("x-amz-content-sha256", contentHashString);

    includeTokenIfPresent(credentials, h);

    List<Parameter> parameters = extractQueryParameters(endpointUrl);
    // don't use Collectors.toMap because it doesn't accept null values in map
    Map<String, String> q = new HashMap<>();
    parameters.forEach(p -> q.put(p.name, p.value));
    String authorization =
        AwsSignatureVersion4.computeSignatureForAuthorizationHeader(
            endpointUrl,
            method.toString(),
            serviceName,
            regionName.orElse("us-east-1"),
            clock,
            h,
            q,
            contentHashString,
            credentials.accessKey(),
            credentials.secretKey());

    // place the computed signature into a formatted 'Authorization' header
    // and call S3
    h.put("Authorization", authorization);
    return httpClient.request(
        endpointUrl, method.toString(), h, requestBody, connectTimeoutMs, readTimeoutMs);
  }

  private static List<Parameter> extractQueryParameters(URL endpointUrl) {
    String query = endpointUrl.getQuery();
    if (query == null) {
      return Collections.emptyList();
    } else {
      return extractQueryParameters(query);
    }
  }

  private static final char QUERY_PARAMETER_SEPARATOR = '&';
  private static final char QUERY_PARAMETER_VALUE_SEPARATOR = '=';

  /**
   * Extract parameters from a query string, preserving encoding.
   *
   * <p>We can't use Apache HTTP Client's URLEncodedUtils.parse, mainly because we don't want to
   * decode names/values.
   *
   * @param rawQuery the query to parse
   * @return The list of parameters, in the order they were found.
   */
  // VisibleForTesting
  static List<Parameter> extractQueryParameters(String rawQuery) {
    List<Parameter> results = new ArrayList<>();
    int endIndex = rawQuery.length() - 1;
    int index = 0;
    while (index <= endIndex) {
      /*
       * Ideally we should first look for '&', then look for '=' before the '&', but
       * obviously that's not how AWS understand query parsing; see the test
       * "post-vanilla-query-nonunreserved" in the test suite. A string such as
       * "?foo&bar=qux" will be understood as one parameter with name "foo&bar" and
       * value "qux". Don't ask me why.
       */
      String name;
      String value;
      int nameValueSeparatorIndex = rawQuery.indexOf(QUERY_PARAMETER_VALUE_SEPARATOR, index);
      if (nameValueSeparatorIndex < 0) {
        // No value
        name = rawQuery.substring(index);
        value = null;

        index = endIndex + 1;
      } else {
        int parameterSeparatorIndex =
            rawQuery.indexOf(QUERY_PARAMETER_SEPARATOR, nameValueSeparatorIndex);
        if (parameterSeparatorIndex < 0) {
          parameterSeparatorIndex = endIndex + 1;
        }
        name = rawQuery.substring(index, nameValueSeparatorIndex);
        value = rawQuery.substring(nameValueSeparatorIndex + 1, parameterSeparatorIndex);

        index = parameterSeparatorIndex + 1;
      }
      // note that value = null is valid as we can have a parameter without a value in
      // a query string (legal http)
      results.add(parameter(name, value, "UTF-8"));
    }
    return results;
  }

  // VisibleForTesting
  static Parameter parameter(String name, String value, String charset) {
    try {
      return new Parameter(
          URLDecoder.decode(name, charset),
          value == null ? value : URLDecoder.decode(value, charset));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  // VisibleForTesting
  static final class Parameter {
    final String name;
    final String value;

    Parameter(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  static boolean isEmpty(byte[] array) {
    return array == null || array.length == 0;
  }
}
