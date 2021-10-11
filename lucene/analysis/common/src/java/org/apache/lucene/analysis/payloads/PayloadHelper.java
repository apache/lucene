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
package org.apache.lucene.analysis.payloads;

import org.apache.lucene.util.BitUtil;

/** Utility methods for encoding payloads. */
public class PayloadHelper {

  public static byte[] encodeFloat(float payload) {
    return encodeFloat(payload, new byte[4], 0);
  }

  public static byte[] encodeFloat(float payload, byte[] data, int offset) {
    BitUtil.VH_BE_FLOAT.set(data, offset, payload);
    return data;
  }

  public static byte[] encodeInt(int payload) {
    return encodeInt(payload, new byte[4], 0);
  }

  public static byte[] encodeInt(int payload, byte[] data, int offset) {
    BitUtil.VH_BE_INT.set(data, offset, payload);
    return data;
  }

  /**
   * @see #decodeFloat(byte[], int)
   * @see #encodeFloat(float)
   * @return the decoded float
   */
  public static float decodeFloat(byte[] bytes) {
    return decodeFloat(bytes, 0);
  }

  /**
   * Decode the payload that was encoded using {@link #encodeFloat(float)}. NOTE: the length of the
   * array must be at least offset + 4 long.
   *
   * @param bytes The bytes to decode
   * @param offset The offset into the array.
   * @return The float that was encoded
   * @see #encodeFloat(float)
   */
  public static final float decodeFloat(byte[] bytes, int offset) {
    return (float) BitUtil.VH_BE_FLOAT.get(bytes, offset);
  }

  public static final int decodeInt(byte[] bytes, int offset) {
    return (int) BitUtil.VH_BE_INT.get(bytes, offset);
  }
}
