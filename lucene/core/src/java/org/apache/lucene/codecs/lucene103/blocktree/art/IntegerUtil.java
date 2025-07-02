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
package org.apache.lucene.codecs.lucene103.blocktree.art;

public class IntegerUtil {

  /**
   * convert integer to its byte array format
   * @param v an input integer value
   * @return the big endian byte array representation
   */
  public static byte[] toBDBytes(int v) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (v >> 24);
    bytes[1] = (byte) (v >> 16);
    bytes[2] = (byte) (v >> 8);
    bytes[3] = (byte) v;
    return bytes;
  }

  /**
   * convert into its integer representation
   * @param bytes the big endian integer's byte array
   * @return a integer corresponding to input bytes
   */
  public static int fromBDBytes(byte[] bytes) {
    return (bytes[0] & 0xFF) << 24
        | (bytes[1] & 0xFF) << 16
        | (bytes[2] & 0xFF) << 8
        | bytes[3] & 0xFF;
  }

  /**
   * set a specified position byte to another value to return a fresh integer
   * @param v the input integer value
   * @param bv the byte value to replace
   * @param pos the position of an 8 byte array to replace
   * @return a fresh integer after a specified position byte been replaced
   */
  public static int setByte(int v, byte bv, int pos) {
    byte[] bytes = toBDBytes(v);
    bytes[pos] = bv;
    return fromBDBytes(bytes);
  }

  /**
   * shift the byte left from the specified position
   * @param v a integer value
   * @param pos the position from which to shift byte values left
   * @param count the shifting numbers
   * @return a fresh integer value
   */
  public static int shiftLeftFromSpecifiedPosition(int v, int pos, int count) {
    byte[] initialVal = toBDBytes(v);
    System.arraycopy(initialVal, pos + 1, initialVal, pos, count);
    return fromBDBytes(initialVal);
  }

  /**
   * fetch the first byte
   * @param v an input integer
   * @return the first byte of the big endian representation
   */
  public static byte firstByte(int v) {
    return (byte) (v >> 24);
  }
}
