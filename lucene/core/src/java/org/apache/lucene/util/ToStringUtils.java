/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

/** Helper methods to ease implementing {@link Object#toString()}. */
public final class ToStringUtils {

  private ToStringUtils() {} // no instance

  public static void byteArray(StringBuilder buffer, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      buffer.append("b[").append(i).append("]=").append(bytes[i]);
      if (i < bytes.length - 1) {
        buffer.append(',');
      }
    }
  }

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /**
   * Unlike {@link Long#toHexString(long)} returns a String with a "0x" prefix and all the leading
   * zeros.
   */
  public static String longHex(long x) {
    char[] asHex = new char[16];
    for (int i = 16; --i >= 0; x >>>= 4) {
      asHex[i] = HEX[(int) x & 0x0F];
    }
    return "0x" + new String(asHex);
  }

  /**
   * Builds a String with both textual representation of the {@link BytesRef} data and the bytes hex
   * values. For example: {@code "hello [68 65 6c 6c 6f]"}. If the content is not a valid UTF-8
   * sequence, only the bytes hex values are returned, as per {@link BytesRef#toString()}.
   */
  @SuppressWarnings("unused")
  public static String bytesRefToString(BytesRef b) {
    if (b == null) {
      return "null";
    }
    try {
      return b.utf8ToString() + " " + b;
    } catch (AssertionError | RuntimeException t) {
      // If BytesRef isn't actually UTF-8, or it's e.g. a prefix of UTF-8
      // that ends mid-unicode-char, we fall back to hex:
      return b.toString();
    }
  }

  public static String bytesRefToString(BytesRefBuilder b) {
    return bytesRefToString(b.get());
  }

  public static String bytesRefToString(byte[] b) {
    return bytesRefToString(new BytesRef(b));
  }
}
