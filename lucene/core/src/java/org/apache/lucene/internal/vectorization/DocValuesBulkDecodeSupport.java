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
package org.apache.lucene.internal.vectorization;

import org.apache.lucene.util.LongValues;

/**
 * Interface for SIMD-accelerated doc values bulk decode operations.
 *
 * <p>Implementations decode byte-aligned packed numeric values into a {@code long[]} destination.
 * The default scalar implementation is used when the Panama Vector API is unavailable; a
 * SIMD-accelerated implementation is used otherwise.
 *
 * @lucene.internal
 */
public interface DocValuesBulkDecodeSupport {

  /**
   * Decodes {@code count} byte-aligned packed values from {@code bytes} into {@code values}.
   *
   * @param bytes source bytes encoded like {@link LongValues} produced by packed {@code
   *     DirectReader}
   * @param bytesOffset first byte to read
   * @param bitsPerValue number of bits per value, must be a multiple of 8
   * @param values destination values
   * @param valuesOffset first destination slot
   * @param count number of values to decode
   */
  void decodeByteAligned(
      byte[] bytes, int bytesOffset, int bitsPerValue, long[] values, int valuesOffset, int count);
}
