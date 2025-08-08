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

import static org.apache.lucene.internal.vectorization.PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE;
import static org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport.fma;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/** Implementations of bulk vector comparison operations. Currently only supports float32. */
public final class MemorySegmentBulkVectorOps {

  static final VectorSpecies<Float> FLOAT_SPECIES =
      VectorSpecies.of(float.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
  static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
  static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(LE);

  private MemorySegmentBulkVectorOps() {}

  sealed interface FloatVectorLoader {
    /** Returns the number of float elements. */
    int length();

    FloatVector load(VectorSpecies<Float> species, int index);

    float tail(int index);
  }

  record FloatArrayLoader(float[] arr) implements FloatVectorLoader {
    @Override
    public int length() {
      return arr.length;
    }

    @Override
    public FloatVector load(VectorSpecies<Float> species, int index) {
      // assert index + species.length() <= length();
      return FloatVector.fromArray(species, arr, index);
    }

    @Override
    public float tail(int index) {
      // assert index <= length();
      return arr[index];
    }
  }

  record FloatMemorySegmentLoader(MemorySegment segment) implements FloatVectorLoader {
    @Override
    public int length() {
      return Math.toIntExact(segment.byteSize() / Float.BYTES);
    }

    @Override
    public FloatVector load(VectorSpecies<Float> species, int index) {
      // assert index + species.length() <= length();
      return FloatVector.fromMemorySegment(species, segment, (long) index * Float.BYTES, LE);
    }

    @Override
    public float tail(int index) {
      // assert index <= length();
      return segment.get(LAYOUT_LE_FLOAT, (long) index * Float.BYTES);
    }
  }

  public static final class DotFromQueryArray extends AbstractDotProduct {

    public void dotProductBulk(
        float[] scores,
        float[] q,
        MemorySegment d1,
        MemorySegment d2,
        MemorySegment d3,
        MemorySegment d4,
        int elementCount) {
      super.dotProductBulk(scores, new FloatArrayLoader(q), d1, d2, d3, d4, elementCount);
    }
  }

  public static final class DotFromQuerySegment extends AbstractDotProduct {

    public void dotProductBulk(
        float[] scores,
        MemorySegment q,
        MemorySegment d1,
        MemorySegment d2,
        MemorySegment d3,
        MemorySegment d4,
        int elementCount) {
      super.dotProductBulk(scores, new FloatMemorySegmentLoader(q), d1, d2, d3, d4, elementCount);
    }

    public float dotProduct(MemorySegment q, MemorySegment d, int elementCount) {
      int i = 0;
      FloatVector sv = FloatVector.zero(FLOAT_SPECIES);
      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, q, offset, LE);
        FloatVector dv = FloatVector.fromMemorySegment(FLOAT_SPECIES, d, offset, LE);
        sv = fma(qv, dv, sv);
      }
      float score = sv.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        score += q.get(LAYOUT_LE_FLOAT, offset) * d.get(LAYOUT_LE_FLOAT, offset);
      }
      return score;
    }
  }

  abstract static class AbstractDotProduct {

    protected void dotProductBulk(
        float[] scores,
        FloatVectorLoader q,
        MemorySegment d1,
        MemorySegment d2,
        MemorySegment d3,
        MemorySegment d4,
        int elementCount) {
      int i = 0;
      FloatVector sv1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv4 = FloatVector.zero(FLOAT_SPECIES);

      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector dv1 = FloatVector.fromMemorySegment(FLOAT_SPECIES, d1, offset, LE);
        FloatVector dv2 = FloatVector.fromMemorySegment(FLOAT_SPECIES, d2, offset, LE);
        FloatVector dv3 = FloatVector.fromMemorySegment(FLOAT_SPECIES, d3, offset, LE);
        FloatVector dv4 = FloatVector.fromMemorySegment(FLOAT_SPECIES, d4, offset, LE);
        FloatVector qv = q.load(FLOAT_SPECIES, i);
        sv1 = fma(qv, dv1, sv1);
        sv2 = fma(qv, dv2, sv2);
        sv3 = fma(qv, dv3, sv3);
        sv4 = fma(qv, dv4, sv4);
      }
      float sum1 = sv1.reduceLanes(VectorOperators.ADD);
      float sum2 = sv2.reduceLanes(VectorOperators.ADD);
      float sum3 = sv3.reduceLanes(VectorOperators.ADD);
      float sum4 = sv4.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        final float qValue = q.tail(i);
        sum1 = fma(qValue, d1.get(LAYOUT_LE_FLOAT, offset), sum1);
        sum2 = fma(qValue, d2.get(LAYOUT_LE_FLOAT, offset), sum2);
        sum3 = fma(qValue, d3.get(LAYOUT_LE_FLOAT, offset), sum3);
        sum4 = fma(qValue, d4.get(LAYOUT_LE_FLOAT, offset), sum4);
      }
      scores[0] = sum1;
      scores[1] = sum2;
      scores[2] = sum3;
      scores[3] = sum4;
    }
  }
}
