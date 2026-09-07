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

/** Util to convert long to bytes, or bytes to long. */
public class LongUtils {
  /**
   * to big endian bytes representation
   *
   * @param v a long value
   * @return the input long value's big endian byte array representation
   */
  public static byte[] toBDBytes(long v) {
    byte[] work = new byte[8];
    work[7] = (byte) v;
    work[6] = (byte) (v >> 8);
    work[5] = (byte) (v >> 16);
    work[4] = (byte) (v >> 24);
    work[3] = (byte) (v >> 32);
    work[2] = (byte) (v >> 40);
    work[1] = (byte) (v >> 48);
    work[0] = (byte) (v >> 56);
    return work;
  }

  /**
   * get the long from the big endian representation bytes
   *
   * @param work the byte array
   * @return the long data
   */
  public static long fromBDBytes(byte[] work) {
    return (long) (work[0]) << 56
        /* long cast needed or shift done modulo 32 */
        | (long) (work[1] & 0xff) << 48
        | (long) (work[2] & 0xff) << 40
        | (long) (work[3] & 0xff) << 32
        | (long) (work[4] & 0xff) << 24
        | (long) (work[5] & 0xff) << 16
        | (long) (work[6] & 0xff) << 8
        | (long) (work[7] & 0xff);
  }

  /**
   * initialize a long value with the given fist 32 bit
   *
   * @param v first 32 bit value
   * @return a long value
   */
  public static long initWithFirst4Byte(int v) {
    return ((long) v) << 32;
  }
}
