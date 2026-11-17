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

  public static final DotProduct DOT_INSTANCE = new DotProduct();
  public static final Cosine COS_INSTANCE = new Cosine();
  public static final SqrDistance SQR_INSTANCE = new SqrDistance();

  private MemorySegmentBulkVectorOps() {}

  public static final class DotProduct {

    private DotProduct() {}

    public void dotProductBulk(
        MemorySegment dataSeg,
        float[] scores,
        float[] q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.dotProductBulk(dataSeg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public void dotProductBulk(
        MemorySegment seg,
        float[] scores,
        long q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.dotProductBulk(seg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public float dotProduct(MemorySegment seg, long q, long d, int elementCount) {
      int i = 0;
      FloatVector sv = FloatVector.zero(FLOAT_SPECIES);
      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, q + offset, LE);
        FloatVector dv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d + offset, LE);
        sv = fma(qv, dv, sv);
      }
      float score = sv.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        score += seg.get(LAYOUT_LE_FLOAT, q + offset) * seg.get(LAYOUT_LE_FLOAT, d + offset);
      }
      return score;
    }
  }

  // -- cosine

  public static final class Cosine {

    private Cosine() {}

    public void cosineBulk(
        MemorySegment dataSeg,
        float[] scores,
        float[] q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.cosineBulk(dataSeg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public void cosineBulk(
        MemorySegment seg,
        float[] scores,
        long q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.cosineBulk(seg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public float cosine(MemorySegment seg, long q, long d, int elementCount) {
      int i = 0;
      FloatVector sv = FloatVector.zero(FLOAT_SPECIES);
      FloatVector qvNorm = FloatVector.zero(FLOAT_SPECIES);
      FloatVector dvNorm = FloatVector.zero(FLOAT_SPECIES);
      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, q + offset, LE);
        FloatVector dv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d + offset, LE);
        sv = fma(qv, dv, sv);
        qvNorm = fma(qv, qv, qvNorm);
        dvNorm = fma(dv, dv, dvNorm);
      }
      float sum = sv.reduceLanes(VectorOperators.ADD);
      float qNorm = qvNorm.reduceLanes(VectorOperators.ADD);
      float dNorm = dvNorm.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        final float qValue = seg.get(LAYOUT_LE_FLOAT, q + offset);
        final float dValue = seg.get(LAYOUT_LE_FLOAT, d + offset);
        sum = fma(qValue, dValue, sum);
        qNorm = fma(qValue, qValue, qNorm);
        dNorm = fma(dValue, dValue, dNorm);
      }
      return (float) (sum / Math.sqrt((double) qNorm * (double) dNorm));
    }
  }

  // -- square distance

  public static final class SqrDistance {

    private SqrDistance() {}

    public void sqrDistanceBulk(
        MemorySegment dataSeg,
        float[] scores,
        float[] q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.squareDistanceBulk(dataSeg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public void sqrDistanceBulk(
        MemorySegment seg,
        float[] scores,
        long q,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      PanamaVectorUtilSupport.squareDistanceBulk(seg, scores, q, d1, d2, d3, d4, elementCount);
    }

    public float sqrDistance(MemorySegment seg, long q, long d, int elementCount) {
      int i = 0;
      FloatVector sv = FloatVector.zero(FLOAT_SPECIES);
      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, q + offset, LE);
        FloatVector dv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d + offset, LE);
        FloatVector diff = qv.sub(dv);
        sv = fma(diff, diff, sv);
      }
      float score = sv.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        float diff = seg.get(LAYOUT_LE_FLOAT, q + offset) - seg.get(LAYOUT_LE_FLOAT, d + offset);
        score = fma(diff, diff, score);
      }
      return score;
    }
  }
}
