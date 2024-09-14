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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

/**
 * Optimized scalar quantized implementation of {@link FlatVectorsScorer} for quantized vectors
 * stored in the Lucene99 format.
 *
 * @lucene.experimental
 */
public class Lucene99MemorySegmentScalarQuantizedVectorScorer {

  static RandomVectorScorer fromVectorSimilarity(
      byte[] targetBytes,
      float offsetCorrection,
      VectorSimilarityFunction sim,
      float constMultiplier,
      QuantizedByteVectorValues values) {

    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(values, constMultiplier, targetBytes);
      case COSINE, DOT_PRODUCT ->
          dotProductFactory(
              targetBytes,
              offsetCorrection,
              constMultiplier,
              values,
              f -> Math.max((1 + f) / 2, 0));
      case MAXIMUM_INNER_PRODUCT ->
          dotProductFactory(
              targetBytes,
              offsetCorrection,
              constMultiplier,
              values,
              VectorUtil::scaleMaxInnerProductScore);
    };
  }

  private static RandomVectorScorer.AbstractRandomVectorScorer dotProductFactory(
      byte[] targetBytes,
      float offsetCorrection,
      float constMultiplier,
      QuantizedByteVectorValues values,
      FloatToFloatFunction scoreAdjustmentFunction) {
    if (values.getScalarQuantizer().getBits() <= 4) {
      if (values.getVectorByteLength() != values.dimension() && values.getSlice() != null) {
        return new CompressedInt4DotProduct(
            values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
      }
      return new Int4DotProduct(
          values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
    }
    return new DotProduct(
        values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
  }

  private static class Euclidean extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final byte[] targetBytes;
    private final QuantizedByteVectorValues values;

    private Euclidean(QuantizedByteVectorValues values, float constMultiplier, byte[] targetBytes) {
      super(values);
      this.values = values;
      this.constMultiplier = constMultiplier;
      this.targetBytes = targetBytes;
    }

    @Override
    public float score(int node) throws IOException {
      byte[] nodeVector = values.vectorValue(node);
      int squareDistance = VectorUtil.squareDistance(nodeVector, targetBytes);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  private static class DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private MemorySegment scratch;

    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;
    private final MemorySegment targetBytes;
    private final MemorySegmentAccessInput msInput;

    private DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        MemorySegment targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
      this.targetBytes = targetBytes;
      IndexInput input = FilterIndexInput.unwrapOnlyTest(values.getSlice());
      if (input instanceof MemorySegmentAccessInput) {
        this.msInput = (MemorySegmentAccessInput) input;
      } else {
        this.msInput = null;
      }
    }

    private DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
      IndexInput input = FilterIndexInput.unwrapOnlyTest(values.getSlice());
      if (input instanceof MemorySegmentAccessInput) {
        this.msInput = (MemorySegmentAccessInput) input;
      } else {
        this.msInput = null;
      }
      if (Constants.NATIVE_DOT_PRODUCT_ENABLED == true && msInput != null) {
        this.targetBytes =
            Arena.ofAuto()
                .allocate(values.getVectorByteLength(), ValueLayout.JAVA_BYTE.byteAlignment());
        MemorySegment.copy(
            targetBytes, 0, this.targetBytes, ValueLayout.JAVA_BYTE, 0, targetBytes.length);
      } else {
        this.targetBytes = MemorySegment.ofArray(targetBytes);
      }
    }

    private MemorySegment vectorValue(int vectorOrdinal) throws IOException {
      if (Constants.NATIVE_DOT_PRODUCT_ENABLED == false || msInput == null) {
        return MemorySegment.ofArray(values.vectorValue(vectorOrdinal));
      }

      int vectorByteSize = values.getVectorByteLength();
      int scoreCorrectionBytes = Float.BYTES;
      long byteOffset = (long) vectorOrdinal * (vectorByteSize + scoreCorrectionBytes);
      MemorySegment seg = msInput.segmentSliceOrNull(byteOffset, vectorByteSize);
      if (seg == null) {
        if (scratch == null) {
          scratch =
              Arena.ofAuto()
                  .allocate(values.getVectorByteLength(), ValueLayout.JAVA_BYTE.byteAlignment());
        }
        msInput.readBytes(byteOffset, scratch, 0, vectorByteSize);
        seg = scratch;
      }
      return seg;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      MemorySegment storedVector = vectorValue(vectorOrdinal);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = PanamaVectorUtilSupport.dotProduct(storedVector, targetBytes);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }
  }

  private static class CompressedInt4DotProduct
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    private CompressedInt4DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.compressedVector = new byte[values.getVectorByteLength()];
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      // get compressed vector, in Lucene99, vector values are stored and have a single value for
      // offset correction
      values.getSlice().seek((long) vectorOrdinal * (values.getVectorByteLength() + Float.BYTES));
      values.getSlice().readBytes(compressedVector, 0, compressedVector.length);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.int4DotProductPacked(targetBytes, compressedVector);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }
  }

  private static class Int4DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public Int4DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      byte[] storedVector = values.vectorValue(vectorOrdinal);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.int4DotProduct(storedVector, targetBytes);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }
  }

  @FunctionalInterface
  interface FloatToFloatFunction {
    float apply(float f);
  }

  static final class ScalarQuantizedRandomVectorScorerSupplier
      implements RandomVectorScorerSupplier {

    private MemorySegment scratch;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final FloatToFloatFunction scoreAdjustmentFunction;
    private final QuantizedByteVectorValues values;
    private final QuantizedByteVectorValues values1;
    private final QuantizedByteVectorValues values2;
    private final MemorySegmentAccessInput msInput;

    public ScalarQuantizedRandomVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {
      this.values = values;
      this.values1 = values.copy();
      this.values2 = values.copy();
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      IndexInput input = FilterIndexInput.unwrapOnlyTest(values.getSlice());
      if (input instanceof MemorySegmentAccessInput) {
        this.msInput = (MemorySegmentAccessInput) input;
      } else {
        this.msInput = null;
      }
      this.scoreAdjustmentFunction =
          switch (vectorSimilarityFunction) {
            case COSINE, DOT_PRODUCT -> (f) -> Math.max((1 + f) / 2, 0);
            case MAXIMUM_INNER_PRODUCT -> VectorUtil::scaleMaxInnerProductScore;
            default -> null;
          };
    }

    /**
     * Reads vector bytes for the supplied ordinal and returns them as a MemorySegment. if
     * NATIVE_DOT_PRODUCT is not enabled, then vector bytes will be copied from the underlying
     * memory-mapped files to heap allocated byte[] and wrapped into MemorySegment. In case
     * NATIVE_DOT_PRODUCT is enabled, a slice of the MemorySegment over vector bytes is returned. NO
     * memory-copy happens when vector bytes are fully contained within a single MemorySegment
     * (common case) that memory-maps a portion of the underlying file.
     *
     * @param vectorOrdinal
     * @return
     * @throws IOException
     */
    private MemorySegment vectorValue(int vectorOrdinal) throws IOException {
      if (Constants.NATIVE_DOT_PRODUCT_ENABLED == false || msInput == null) {
        return MemorySegment.ofArray(values1.vectorValue(vectorOrdinal));
      }
      int vectorByteSize = values1.getVectorByteLength();
      int scoreCorrectionBytes = Float.BYTES;
      long byteOffset = (long) vectorOrdinal * (vectorByteSize + scoreCorrectionBytes);
      MemorySegment seg = msInput.segmentSliceOrNull(byteOffset, vectorByteSize);
      if (seg == null) {
        if (scratch == null) {
          scratch =
              Arena.ofAuto()
                  .allocate(values.getVectorByteLength(), ValueLayout.JAVA_BYTE.byteAlignment());
        }
        msInput.readBytes(byteOffset, scratch, 0, vectorByteSize);
        seg = scratch;
      }
      return seg;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      float constMultiplier = values1.getScalarQuantizer().getConstantMultiplier();
      float offsetCorrection = values1.getScoreCorrectionConstant(ord);

      return switch (vectorSimilarityFunction) {
        case EUCLIDEAN -> new Euclidean(values2, constMultiplier, values1.vectorValue(ord));
        case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> {
          if (values2.getScalarQuantizer().getBits() <= 4) {
            // Use old APIs for dot-product on int4 or compressed vectors
            if (values2.getVectorByteLength() != values2.dimension()
                && values2.getSlice() != null) {
              yield new CompressedInt4DotProduct(
                  values2,
                  constMultiplier,
                  values1.vectorValue(ord),
                  offsetCorrection,
                  scoreAdjustmentFunction);
            }
            yield new Int4DotProduct(
                values2,
                constMultiplier,
                values1.vectorValue(ord),
                offsetCorrection,
                scoreAdjustmentFunction);
          } else {
            // Use new APIs to construct slice of MemorySegment and dot-product in native code
            yield new DotProduct(
                values2,
                constMultiplier,
                vectorValue(ord),
                offsetCorrection,
                scoreAdjustmentFunction);
          }
        }
      };
    }

    @Override
    public ScalarQuantizedRandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedRandomVectorScorerSupplier(values.copy(), vectorSimilarityFunction);
    }

    @Override
    public String toString() {
      return "ScalarQuantizedRandomVectorScorerSupplier(vectorSimilarityFunction="
          + vectorSimilarityFunction
          + ")";
    }
  }
}
