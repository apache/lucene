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
      dotProductBulkImpl(dataSeg, scores, q, -1L, d1, d2, d3, d4, elementCount);
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
      dotProductBulkImpl(seg, scores, null, q, d1, d2, d3, d4, elementCount);
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

    private void dotProductBulkImpl(
        MemorySegment seg,
        float[] scores,
        float[] qArray,
        long qOffset,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      int i = 0;
      FloatVector sv1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv4 = FloatVector.zero(FLOAT_SPECIES);

      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector dv1 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d1 + offset, LE);
        FloatVector dv2 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d2 + offset, LE);
        FloatVector dv3 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d3 + offset, LE);
        FloatVector dv4 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d4 + offset, LE);
        FloatVector qv;
        if (qOffset == -1L) {
          qv = FloatVector.fromArray(FLOAT_SPECIES, qArray, i);
        } else {
          qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, qOffset + offset, LE);
        }
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
        float qValue;
        if (qOffset == -1L) {
          qValue = qArray[i];
        } else {
          qValue = seg.get(LAYOUT_LE_FLOAT, qOffset + offset);
        }
        sum1 = fma(qValue, seg.get(LAYOUT_LE_FLOAT, d1 + offset), sum1);
        sum2 = fma(qValue, seg.get(LAYOUT_LE_FLOAT, d2 + offset), sum2);
        sum3 = fma(qValue, seg.get(LAYOUT_LE_FLOAT, d3 + offset), sum3);
        sum4 = fma(qValue, seg.get(LAYOUT_LE_FLOAT, d4 + offset), sum4);
      }
      scores[0] = sum1;
      scores[1] = sum2;
      scores[2] = sum3;
      scores[3] = sum4;
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
      cosineBulkImpl(dataSeg, scores, q, -1L, d1, d2, d3, d4, elementCount);
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
      cosineBulkImpl(seg, scores, null, q, d1, d2, d3, d4, elementCount);
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

    private void cosineBulkImpl(
        MemorySegment seg,
        float[] scores,
        float[] qArray,
        long qOffset,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      int i = 0;
      FloatVector sv1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv4 = FloatVector.zero(FLOAT_SPECIES);

      FloatVector qvNorm = FloatVector.zero(FLOAT_SPECIES);
      FloatVector dv1Norm = FloatVector.zero(FLOAT_SPECIES);
      FloatVector dv2Norm = FloatVector.zero(FLOAT_SPECIES);
      FloatVector dv3Norm = FloatVector.zero(FLOAT_SPECIES);
      FloatVector dv4Norm = FloatVector.zero(FLOAT_SPECIES);

      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector dv1 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d1 + offset, LE);
        FloatVector dv2 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d2 + offset, LE);
        FloatVector dv3 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d3 + offset, LE);
        FloatVector dv4 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d4 + offset, LE);
        FloatVector qv;
        if (qOffset == -1L) {
          qv = FloatVector.fromArray(FLOAT_SPECIES, qArray, i);
        } else {
          qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, qOffset + offset, LE);
        }
        qvNorm = fma(qv, qv, qvNorm);
        dv1Norm = fma(dv1, dv1, dv1Norm);
        sv1 = fma(qv, dv1, sv1);
        dv2Norm = fma(dv2, dv2, dv2Norm);
        sv2 = fma(qv, dv2, sv2);
        dv3Norm = fma(dv3, dv3, dv3Norm);
        sv3 = fma(qv, dv3, sv3);
        dv4Norm = fma(dv4, dv4, dv4Norm);
        sv4 = fma(qv, dv4, sv4);
      }
      float sum1 = sv1.reduceLanes(VectorOperators.ADD);
      float sum2 = sv2.reduceLanes(VectorOperators.ADD);
      float sum3 = sv3.reduceLanes(VectorOperators.ADD);
      float sum4 = sv4.reduceLanes(VectorOperators.ADD);
      float qNorm = qvNorm.reduceLanes(VectorOperators.ADD);
      float d1Norm = dv1Norm.reduceLanes(VectorOperators.ADD);
      float d2Norm = dv2Norm.reduceLanes(VectorOperators.ADD);
      float d3Norm = dv3Norm.reduceLanes(VectorOperators.ADD);
      float d4Norm = dv4Norm.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        final float qValue;
        if (qOffset == -1L) {
          qValue = qArray[i];
        } else {
          qValue = seg.get(LAYOUT_LE_FLOAT, qOffset + offset);
        }
        final float d1Value = seg.get(LAYOUT_LE_FLOAT, d1 + offset);
        final float d2Value = seg.get(LAYOUT_LE_FLOAT, d2 + offset);
        final float d3Value = seg.get(LAYOUT_LE_FLOAT, d3 + offset);
        final float d4Value = seg.get(LAYOUT_LE_FLOAT, d4 + offset);
        sum1 = fma(qValue, d1Value, sum1);
        sum2 = fma(qValue, d2Value, sum2);
        sum3 = fma(qValue, d3Value, sum3);
        sum4 = fma(qValue, d4Value, sum4);
        qNorm = fma(qValue, qValue, qNorm);
        d1Norm = fma(d1Value, d1Value, d1Norm);
        d2Norm = fma(d2Value, d2Value, d2Norm);
        d3Norm = fma(d3Value, d3Value, d3Norm);
        d4Norm = fma(d4Value, d4Value, d4Norm);
      }
      scores[0] = (float) (sum1 / Math.sqrt((double) qNorm * (double) d1Norm));
      scores[1] = (float) (sum2 / Math.sqrt((double) qNorm * (double) d2Norm));
      scores[2] = (float) (sum3 / Math.sqrt((double) qNorm * (double) d3Norm));
      scores[3] = (float) (sum4 / Math.sqrt((double) qNorm * (double) d4Norm));
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
      sqrDistanceBulkImpl(dataSeg, scores, q, -1L, d1, d2, d3, d4, elementCount);
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
      sqrDistanceBulkImpl(seg, scores, null, q, d1, d2, d3, d4, elementCount);
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

    private void sqrDistanceBulkImpl(
        MemorySegment seg,
        float[] scores,
        float[] qArray,
        long qOffset,
        long d1,
        long d2,
        long d3,
        long d4,
        int elementCount) {
      int i = 0;
      FloatVector sv1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sv4 = FloatVector.zero(FLOAT_SPECIES);

      final int limit = FLOAT_SPECIES.loopBound(elementCount);
      for (; i < limit; i += FLOAT_SPECIES.length()) {
        final long offset = (long) i * Float.BYTES;
        FloatVector dv1 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d1 + offset, LE);
        FloatVector dv2 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d2 + offset, LE);
        FloatVector dv3 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d3 + offset, LE);
        FloatVector dv4 = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, d4 + offset, LE);
        FloatVector qv;
        if (qOffset == -1L) {
          qv = FloatVector.fromArray(FLOAT_SPECIES, qArray, i);
        } else {
          qv = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, qOffset + offset, LE);
        }
        FloatVector diff1 = qv.sub(dv1);
        FloatVector diff2 = qv.sub(dv2);
        FloatVector diff3 = qv.sub(dv3);
        FloatVector diff4 = qv.sub(dv4);
        sv1 = fma(diff1, diff1, sv1);
        sv2 = fma(diff2, diff2, sv2);
        sv3 = fma(diff3, diff3, sv3);
        sv4 = fma(diff4, diff4, sv4);
      }
      float sum1 = sv1.reduceLanes(VectorOperators.ADD);
      float sum2 = sv2.reduceLanes(VectorOperators.ADD);
      float sum3 = sv3.reduceLanes(VectorOperators.ADD);
      float sum4 = sv4.reduceLanes(VectorOperators.ADD);

      for (; i < elementCount; i++) {
        final long offset = (long) i * Float.BYTES;
        final float qValue;
        if (qOffset == -1L) {
          qValue = qArray[i];
        } else {
          qValue = seg.get(LAYOUT_LE_FLOAT, qOffset + offset);
        }
        float diff1 = qValue - seg.get(LAYOUT_LE_FLOAT, d1 + offset);
        float diff2 = qValue - seg.get(LAYOUT_LE_FLOAT, d2 + offset);
        float diff3 = qValue - seg.get(LAYOUT_LE_FLOAT, d3 + offset);
        float diff4 = qValue - seg.get(LAYOUT_LE_FLOAT, d4 + offset);
        sum1 = fma(diff1, diff1, sum1);
        sum2 = fma(diff2, diff2, sum2);
        sum3 = fma(diff3, diff3, sum3);
        sum4 = fma(diff4, diff4, sum4);
      }
      scores[0] = sum1;
      scores[1] = sum2;
      scores[2] = sum3;
      scores[3] = sum4;
    }
  }
}
