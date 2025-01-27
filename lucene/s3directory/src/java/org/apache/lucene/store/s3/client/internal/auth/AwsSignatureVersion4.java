package org.apache.lucene.store.s3.client.internal.auth;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.lucene.store.s3.client.internal.Clock;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.store.s3.client.internal.util.Util;

/** Common methods and properties for all AWS4 signer variants */
public final class AwsSignatureVersion4 {

  static final String ALGORITHM_HMAC_SHA256 = "HmacSHA256";

  /** SHA256 hash of an empty request body * */
  public static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

  public static final String SCHEME = "AWS4";
  public static final String ALGORITHM = "HMAC-SHA256";
  public static final String TERMINATOR = "aws4_request";

  /** format strings for the date/time and date stamps required during signing * */
  private static final String ISO8601BasicFormat = "yyyyMMdd'T'HHmmss'Z'";

  private static final String DateStringFormat = "yyyyMMdd";

  private AwsSignatureVersion4() {
    // prevent instantiation
  }

  /**
   * Computes an AWS4 authorization for a request, suitable for embedding in query parameters.
   *
   * @param endpointUrl the url to which the request is being made
   * @param httpMethod the HTTP method (GET, POST, PUT, etc.)
   * @param serviceName the AWS service code (e.g iam)
   * @param regionName the AWS region name
   * @param clock provides a timestamp
   * @param headers The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
   * @param queryParameters Any query parameters that will be added to the endpoint. The parameters
   *     should be specified in canonical format.
   * @param bodyHash Precomputed SHA256 hash of the request body content; this value should also be
   *     set as the header 'X-Amz-Content-SHA256' for non-streaming uploads.
   * @param awsAccessKey The user's AWS Access Key.
   * @param awsSecretKey The user's AWS Secret Key.
   * @param sessionToken The session token if any
   * @return The computed authorization string for the request. This value needs to be set as the
   *     header 'Authorization' on the subsequent HTTP request.
   */
  public static String computeSignatureForQueryAuth(
      URL endpointUrl,
      String httpMethod,
      String serviceName,
      Optional<String> regionName,
      Clock clock,
      Map<String, String> headers,
      Map<String, String> queryParameters,
      String bodyHash,
      String awsAccessKey,
      String awsSecretKey,
      Optional<String> sessionToken) {
    // first get the date and time for the subsequent request, and convert
    // to ISO 8601 format for use in signature generation
    Date now = new Date(clock.time());
    String dateTimeStamp = dateTimeFormat().format(now);

    // make sure "Host" header is added
    String hostHeader = endpointUrl.getHost();
    int port = endpointUrl.getPort();
    if (port > -1) {
      hostHeader = hostHeader.concat(":" + port);
    }
    headers.put("Host", hostHeader);

    // canonicalized headers need to be expressed in the query
    // parameters processed in the signature
    String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
    String canonicalizedHeaders = getCanonicalizedHeaderString(headers);

    // we need scope as part of the query parameters
    String dateStamp = dateStampFormat().format(now);
    String scope =
        dateStamp + "/" + regionName.orElse("us-east-1") + "/" + serviceName + "/" + TERMINATOR;

    // add the fixed authorization params required by Signature V4
    queryParameters.put("X-Amz-Algorithm", SCHEME + "-" + ALGORITHM);
    queryParameters.put("X-Amz-Credential", awsAccessKey + "/" + scope);

    // x-amz-date is now added as a query parameter, but still need to be in ISO8601
    // basic form
    queryParameters.put("X-Amz-Date", dateTimeStamp);

    queryParameters.put("X-Amz-SignedHeaders", canonicalizedHeaderNames);

    if (sessionToken.isPresent()) {
      queryParameters.put("X-Amz-Security-Token", sessionToken.get());
    }

    // build the expanded canonical query parameter string that will go into the
    // signature computation
    String canonicalizedQueryParameters = getCanonicalizedQueryString(queryParameters);

    // express all the header and query parameter data as a canonical request string
    String canonicalRequest =
        getCanonicalRequest(
            endpointUrl,
            httpMethod,
            canonicalizedQueryParameters,
            canonicalizedHeaderNames,
            canonicalizedHeaders,
            bodyHash);

    // construct the string to be signed
    String stringToSign =
        getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);
    //        System.out.println("--------- String to sign -----------");
    //        System.out.println(stringToSign);
    //        System.out.println("------------------------------------");

    // compute the signing key
    byte[] kSecret = (SCHEME + awsSecretKey).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = sign(dateStamp, kSecret);
    byte[] kRegion = sign(regionName.orElse("us-east-1"), kDate);
    byte[] kService = sign(serviceName, kRegion);
    byte[] kSigning = sign(TERMINATOR, kService);
    byte[] signature = sign(stringToSign, kSigning);

    // form up the authorization parameters for the caller to place in the query
    // string
    StringBuilder authString = new StringBuilder();

    authString.append("X-Amz-Algorithm=" + queryParameters.get("X-Amz-Algorithm"));
    authString.append("&X-Amz-Credential=" + queryParameters.get("X-Amz-Credential"));
    authString.append("&X-Amz-Date=" + queryParameters.get("X-Amz-Date"));
    authString.append("&X-Amz-Expires=" + queryParameters.get("X-Amz-Expires"));
    authString.append("&X-Amz-SignedHeaders=" + queryParameters.get("X-Amz-SignedHeaders"));
    authString.append("&X-Amz-Signature=" + Util.toHex(signature));
    if (sessionToken.isPresent()) {
      authString.append("&X-Amz-Security-Token=" + Util.urlEncode(sessionToken.get(), false));
    }
    return authString.toString();
  }

  /**
   * Computes an AWS4 signature for a request, ready for inclusion as an 'Authorization' header.
   *
   * @param endpointUrl the url to which the request is being made
   * @param httpMethod the HTTP method (GET, POST, PUT, etc.)
   * @param serviceName the AWS service code (e.g iam)
   * @param regionName the AWS region name
   * @param clock provides a timestamp
   * @param headers The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
   * @param queryParameters Any query parameters that will be added to the endpoint. The parameters
   *     should be specified in canonical format.
   * @param bodyHash Precomputed SHA256 hash of the request body content; this value should also be
   *     set as the header 'X-Amz-Content-SHA256' for non-streaming uploads.
   * @param awsAccessKey The user's AWS Access Key.
   * @param awsSecretKey The user's AWS Secret Key.
   * @return The computed authorization string for the request. This value needs to be set as the
   *     header 'Authorization' on the subsequent HTTP request.
   */
  public static String computeSignatureForAuthorizationHeader(
      URL endpointUrl,
      String httpMethod,
      String serviceName,
      String regionName,
      Clock clock,
      Map<String, String> headers,
      Map<String, String> queryParameters,
      String bodyHash,
      String awsAccessKey,
      String awsSecretKey) {
    Preconditions.checkNotNull(headers);
    Preconditions.checkNotNull(queryParameters);
    SimpleDateFormat dateTimeFormat = dateTimeFormat();
    SimpleDateFormat dateStampFormat = dateStampFormat();
    // first get the date and time for the subsequent request, and convert
    // to ISO 8601 format for use in signature generation
    Date now = new Date(clock.time());
    String dateTimeStamp = dateTimeFormat.format(now);

    // update the headers with required 'x-amz-date' and 'host' values
    headers.put("x-amz-date", dateTimeStamp);

    String hostHeader = endpointUrl.getHost();
    int port = endpointUrl.getPort();
    if (port > -1) {
      hostHeader = hostHeader.concat(":" + port);
    }
    headers.put("Host", hostHeader);

    // canonicalize the headers; we need the set of header names as well as the
    // names and values to go into the signature process
    String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
    String canonicalizedHeaders = getCanonicalizedHeaderString(headers);

    // if any query string parameters have been supplied, canonicalize them
    String canonicalizedQueryParameters = getCanonicalizedQueryString(queryParameters);
    //        System.out.println("--------- Canonical query string --------");
    //        System.out.println(canonicalizedQueryParameters);

    // canonicalize the various components of the request
    String canonicalRequest =
        getCanonicalRequest(
            endpointUrl,
            httpMethod,
            canonicalizedQueryParameters,
            canonicalizedHeaderNames,
            canonicalizedHeaders,
            bodyHash);
    //        System.out.println("--------- Canonical request --------");
    //        System.out.println(canonicalRequest);
    //        System.out.println("------------------------------------");

    // construct the string to be signed
    String dateStamp = dateStampFormat.format(now);
    String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + TERMINATOR;
    String stringToSign =
        getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);
    //        System.out.println("--------- String to sign -----------");
    //        System.out.println(stringToSign);
    //        System.out.println("------------------------------------");

    // compute the signing key
    byte[] kSecret = (SCHEME + awsSecretKey).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = sign(dateStamp, kSecret);
    byte[] kRegion = sign(regionName, kDate);
    byte[] kService = sign(serviceName, kRegion);
    byte[] kSigning = sign(TERMINATOR, kService);
    byte[] signature = sign(stringToSign, kSigning);

    String credentialsAuthorizationHeader = "Credential=" + awsAccessKey + "/" + scope;
    String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
    String signatureAuthorizationHeader = "Signature=" + Util.toHex(signature);

    String authorizationHeader =
        SCHEME
            + "-"
            + ALGORITHM
            + " "
            + credentialsAuthorizationHeader
            + ", "
            + signedHeadersAuthorizationHeader
            + ", "
            + signatureAuthorizationHeader;
    return authorizationHeader;
  }

  static SimpleDateFormat dateTimeFormat() {
    SimpleDateFormat sdf = new SimpleDateFormat(ISO8601BasicFormat, Locale.US);
    sdf.setTimeZone(new SimpleTimeZone(0, "UTC"));
    return sdf;
  }

  static SimpleDateFormat dateStampFormat() {
    SimpleDateFormat sdf = new SimpleDateFormat(DateStringFormat, Locale.US);
    sdf.setTimeZone(new SimpleTimeZone(0, "UTC"));
    return sdf;
  }

  /**
   * Returns the canonical string of header names that will be included in the signature. For AWS4,
   * all header names must be included in the process in sorted canonicalized order.
   *
   * @param headers input to convert to canonical string
   * @return canonical header names string
   */
  static String getCanonicalizeHeaderNames(Map<String, String> headers) {
    List<String> sortedHeaders = new ArrayList<String>();
    sortedHeaders.addAll(headers.keySet());
    Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

    StringBuilder buffer = new StringBuilder();
    for (String header : sortedHeaders) {
      if (buffer.length() > 0) buffer.append(";");
      buffer.append(header.toLowerCase(Locale.ENGLISH));
    }

    return buffer.toString();
  }

  /**
   * Returns the canonical headers string. For AWS4, all headers must be included in the signing
   * process.
   *
   * @param headers input to convert to canonical string
   * @return canonical headers string
   */
  static String getCanonicalizedHeaderString(Map<String, String> headers) {

    // step1: sort the headers by case-insensitive order
    List<String> sortedHeaders = new ArrayList<String>();
    sortedHeaders.addAll(headers.keySet());
    Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

    // step2: form the canonical header:value entries in sorted order.
    // Multiple white spaces in the values should be compressed to a single
    // space.
    StringBuilder buffer = new StringBuilder();
    for (String key : sortedHeaders) {
      buffer.append(
          key.toLowerCase(Locale.ENGLISH).replaceAll("\\s+", " ")
              + ":"
              + headers.get(key).replaceAll("\\s+", " "));
      buffer.append("\n");
    }

    return buffer.toString();
  }

  /**
   * Returns the canonical request string to go into the signer process; this consists of several
   * canonical sub-parts.
   *
   * @param endpoint url to which the request is being made
   * @param httpMethod http method (e.g GET, POST)
   * @param canonicalQueryParameters canonical query parameters string
   * @param canonicalizedHeaderNames canonical header names string
   * @param canonicalizedHeaders canonical headers string
   * @param bodyHash SHA-256 hash of request body
   * @return canonical request string
   */
  static String getCanonicalRequest(
      URL endpoint,
      String httpMethod,
      String canonicalQueryParameters,
      String canonicalizedHeaderNames,
      String canonicalizedHeaders,
      String bodyHash) {
    return httpMethod
        + "\n" //
        + getCanonicalizedResourcePath(endpoint)
        + "\n" //
        + canonicalQueryParameters
        + "\n" //
        + canonicalizedHeaders
        + "\n" //
        + canonicalizedHeaderNames
        + "\n" //
        + bodyHash;
  }

  /**
   * Returns the canonicalized resource path for the service endpoint.
   *
   * @param endpoint url to which the request is being made
   * @return canonicalized resource path
   */
  static String getCanonicalizedResourcePath(URL endpoint) {
    Preconditions.checkNotNull(endpoint);
    String path = endpoint.getPath();
    if (path.isEmpty()) {
      return "/";
    } else {
      return Util.urlEncode(path, true);
    }
  }

  /**
   * Examines the specified query string parameters and returns a canonicalized form.
   *
   * <p>The canonicalized query string is formed by first sorting all the query string parameters,
   * then URI encoding both the key and value and then joining them, in order, separating key value
   * pairs with an '&'.
   *
   * @param parameters The query string parameters to be canonicalized.
   * @return A canonicalized form for the specified query string parameters.
   */
  static String getCanonicalizedQueryString(Map<String, String> parameters) {
    SortedMap<String, String> sorted = new TreeMap<String, String>();

    for (Entry<String, String> pair : parameters.entrySet()) {
      sorted.put(
          Util.urlEncode(pair.getKey(), false),
          pair.getValue() == null ? null : Util.urlEncode(pair.getValue(), false));
    }

    return sorted //
        .entrySet() //
        .stream() //
        .map(pair -> pair.getKey() + "=" + blankIfNull(pair.getValue()))
        .collect(Collectors.joining("&"));
  }

  private static String blankIfNull(String s) {
    return s == null ? "" : s;
  }

  static String getStringToSign(
      String scheme, String algorithm, String dateTime, String scope, String canonicalRequest) {
    return scheme
        + "-"
        + algorithm
        + "\n"
        + dateTime
        + "\n"
        + scope
        + "\n"
        + Util.toHex(Util.sha256(canonicalRequest));
  }

  static byte[] sign(String stringData, byte[] key) {
    return sign(stringData, key, ALGORITHM_HMAC_SHA256);
  }

  // VisibleForTesting
  static byte[] sign(String stringData, byte[] key, String algorithm) {
    try {
      byte[] data = stringData.getBytes(StandardCharsets.UTF_8);
      Mac mac = Mac.getInstance(algorithm);
      mac.init(new SecretKeySpec(key, algorithm));
      return mac.doFinal(data);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }
}
