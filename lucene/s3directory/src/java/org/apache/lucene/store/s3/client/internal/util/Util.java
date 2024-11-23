package org.apache.lucene.store.s3.client.internal.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/** Utilities for encoding and decoding binary data to and from different forms. */
public final class Util {

  private Util() {
    // prevent instantiation
  }

  public static HttpURLConnection createHttpConnection(
      URL endpointUrl,
      String httpMethod,
      Map<String, String> headers,
      int connectTimeoutMs,
      int readTimeoutMs)
      throws IOException {
    Preconditions.checkNotNull(headers);
    HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
    connection.setRequestMethod(httpMethod);

    for (Entry<String, String> entry : headers.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }

    connection.setUseCaches(false);
    connection.setDoInput(true);
    connection.setDoOutput(true);
    connection.setConnectTimeout(connectTimeoutMs);
    connection.setReadTimeout(readTimeoutMs);
    return connection;
  }

  public static String canonicalMetadataKey(String meta) {
    StringBuilder b = new StringBuilder();
    String s = meta.toLowerCase(Locale.ENGLISH);
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (Character.isDigit(ch) || Character.isAlphabetic(ch)) {
        b.append(ch);
      }
    }
    return b.toString();
  }

  /**
   * Converts byte data to a Hex-encoded string.
   *
   * @param data data to hex encode.
   * @return hex-encoded string.
   */
  public static String toHex(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (int i = 0; i < data.length; i++) {
      String hex = Integer.toHexString(data[i]);
      if (hex.length() == 1) {
        // Append leading zero.
        sb.append("0");
      } else if (hex.length() == 8) {
        // Remove ff prefix from negative numbers.
        hex = hex.substring(6);
      }
      sb.append(hex);
    }
    return sb.toString().toLowerCase(Locale.getDefault());
  }

  public static URL toUrl(String url) {
    try {
      return new URI(url).toURL();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String urlEncode(String url, boolean keepPathSlash) {
    return urlEncode(url, keepPathSlash, "UTF-8");
  }

  // VisibleForTesting
  static String urlEncode(String url, boolean keepPathSlash, String charset) {
    String encoded;
    try {
      encoded = URLEncoder.encode(url, charset).replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    if (keepPathSlash) {
      return encoded.replace("%2F", "/");
    } else {
      return encoded;
    }
  }

  /** Hashes the string contents (assumed to be UTF-8) using the SHA-256 algorithm. */
  public static byte[] sha256(String text) {
    return sha256(text.getBytes(StandardCharsets.UTF_8));
  }

  public static byte[] sha256(byte[] data) {
    return hash(data, "SHA-256");
  }

  // VisibleForTesting
  static byte[] hash(byte[] data, String algorithm) {
    try {
      MessageDigest md = MessageDigest.getInstance(algorithm);
      md.update(data);
      return md.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] readBytesAndClose(InputStream in) {
    try {
      byte[] buffer = new byte[8192];
      int n;
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      while ((n = in.read(buffer)) != -1) {
        bytes.write(buffer, 0, n);
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static final InputStream EMPTY_INPUT_STREAM =
      new InputStream() {
        @Override
        public int read() throws IOException {
          return -1;
        }
      };

  public static final InputStream emptyInputStream() {
    return EMPTY_INPUT_STREAM;
  }
}
