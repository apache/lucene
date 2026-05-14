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

/** Scalar (non-SIMD) implementation of {@link DocValuesRangeSupport}. */
final class DefaultDocValuesRangeSupport implements DocValuesRangeSupport {

  static final DefaultDocValuesRangeSupport INSTANCE = new DefaultDocValuesRangeSupport();

  private DefaultDocValuesRangeSupport() {}

  @Override
  public void rangeIntoBitSet(
      LongValues values,
      int fromDoc,
      int toDoc,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset) {
    // Scalar fallback implementation
    for (int d = fromDoc; d < toDoc; d++) {
      long v = values.get(d);
      if (v >= minValue && v <= maxValue) {
        bitSet.set(d - offset);
      }
    }
  }
}
