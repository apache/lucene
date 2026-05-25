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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;

/**
 * Interface for SIMD-accelerated doc values range operations.
 *
 * <p>Implementations fill a {@link FixedBitSet} with the doc IDs in a range whose values satisfy a
 * numeric range predicate. The default scalar implementation is used when the Panama Vector API is
 * unavailable; a SIMD-accelerated implementation is used otherwise.
 *
 * @lucene.internal
 */
public interface DocValuesRangeSupport {

  /**
   * Fills {@code bitSet} with the doc IDs in {@code [fromDoc, toDoc)} whose values (read via {@code
   * values}) are in {@code [minValue, maxValue]}.
   *
   * @param values random-access reader for the doc values
   * @param fromDoc first doc ID to evaluate (inclusive)
   * @param toDoc last doc ID to evaluate (exclusive)
   * @param minValue lower bound of the range (inclusive)
   * @param maxValue upper bound of the range (inclusive)
   * @param bitSet the bitset to fill
   * @param offset subtracted from each doc ID before setting the bit
   */
  void rangeIntoBitSet(
      LongValues values,
      int fromDoc,
      int toDoc,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset);
}
