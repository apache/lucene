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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.AsymmetricScalarQuantizeFlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

class Lucene104MemorySegmentScalarQuantizedVectorScorer
    implements AsymmetricScalarQuantizeFlatVectorsScorer {
  static final Lucene104MemorySegmentScalarQuantizedVectorScorer INSTANCE =
      new Lucene104MemorySegmentScalarQuantizedVectorScorer();

  private static final FlatVectorsScorer DELEGATE =
      new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE);

  private static final int CORRECTIVE_TERMS_SIZE = Float.BYTES * 3 + Integer.BYTES;

  private Lucene104MemorySegmentScalarQuantizedVectorScorer() {}

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input
        && quantized.getScalarEncoding() != ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE) {
      return new RandomVectorScorerSupplierImpl(similarityFunction, quantized, input);
    }
    return DELEGATE.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues scoringVectors,
      QuantizedByteVectorValues targetVectors)
      throws IOException {
    throw new UnsupportedOperationException("no asymmetric encodings are supported yet");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input
        && quantized.getScalarEncoding() != ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE) {
      return new RandomVectorScorerImpl(similarityFunction, quantized, input, target);
    }
    return DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "Lucene104MemorySegmentScalarQuantizedVectorScorer()";
  }

  private abstract static class RandomVectorScorerBase
      extends RandomVectorScorer.AbstractRandomVectorScorer {

    private final QuantizedByteVectorValues values;
    private final MemorySegmentAccessInput input;
    private final int vectorByteSize;
    private final int nodeSize;
    private final VectorSimilarityFunction similarity;
    private byte[] scratch = null;

    RandomVectorScorerBase(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
        MemorySegmentAccessInput input)
        throws IOException {
      super(values);

      this.values = values;
      this.input = input;
      this.vectorByteSize = values.getVectorByteLength();
      this.nodeSize = this.vectorByteSize + CORRECTIVE_TERMS_SIZE;
      this.similarity = similarityFunction;
      checkInvariants();
    }

    final void checkInvariants() {
      if (input.length() < (long) nodeSize * maxOrd()) {
        throw new IllegalArgumentException("input length is less than expected vector data");
      }
    }

    final void checkOrdinal(int ord) {
      if (ord < 0 || ord >= maxOrd()) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    @SuppressWarnings("restricted")
    MemorySegment getVector(int ord) throws IOException {
      checkOrdinal(ord);
      long byteOffset = (long) ord * nodeSize;
      MemorySegment vector = input.segmentSliceOrNull(byteOffset, vectorByteSize);
      if (vector != null) {
        return vector;
      }

      if (scratch == null) {
        scratch = new byte[vectorByteSize];
      }
      input.readBytes(byteOffset, scratch, 0, vectorByteSize);
      return MemorySegment.ofArray(scratch);
    }

    OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) throws IOException {
      checkOrdinal(ord);
      long byteOffset = (long) ord * nodeSize + vectorByteSize;
      return new OptimizedScalarQuantizer.QuantizationResult(
          Float.intBitsToFloat(input.readInt(byteOffset)),
          Float.intBitsToFloat(input.readInt(byteOffset + Integer.BYTES)),
          Float.intBitsToFloat(input.readInt(byteOffset + Integer.BYTES * 2)),
          input.readInt(byteOffset + Integer.BYTES * 3));
    }

    VectorSimilarityFunction getSimilarity() {
      return similarity;
    }

    QuantizedByteVectorValues getValues() {
      return values;
    }
  }

  private static class RandomVectorScorerImpl extends RandomVectorScorerBase {
    private final byte[] query;
    private final OptimizedScalarQuantizer.QuantizationResult queryCorrectiveTerms;
    private final byte[] scratch;

    RandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
        MemorySegmentAccessInput input,
        float[] target)
        throws IOException {
      super(similarityFunction, values, input);
      Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding scalarEncoding =
          values.getScalarEncoding();
      OptimizedScalarQuantizer quantizer = values.getQuantizer();
      scratch = new byte[values.getVectorByteLength()];
      query = new byte[scalarEncoding.getDiscreteDimensions(target.length)];
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      queryCorrectiveTerms =
          quantizer.scalarQuantize(target, query, scalarEncoding.getBits(), values.getCentroid());
    }

    @Override
    public float score(int node) throws IOException {
      MemorySegment docVector = getVector(node);
      float dotProduct =
          switch (getValues().getScalarEncoding()) {
            case UNSIGNED_BYTE -> PanamaVectorUtilSupport.uint8DotProduct(query, docVector);
            case SEVEN_BIT -> PanamaVectorUtilSupport.dotProduct(query, docVector);
            case PACKED_NIBBLE ->
                PanamaVectorUtilSupport.int4DotProductSinglePacked(query, docVector);
            case SINGLE_BIT_QUERY_NIBBLE ->
                throw new IllegalStateException(
                    "this should be handled by the default implementation");
          };
      // Call getCorrectiveTerms() after computing dot product since corrective terms
      // bytes appear after the vector bytes, so this sequence of calls is more cache
      // friendly.
      return Lucene104ScalarQuantizedVectorScorer.quantizedScore(
          getSimilarity(), getValues(), dotProduct, queryCorrectiveTerms, getCorrectiveTerms(node));
    }
  }

  record RandomVectorScorerSupplierImpl(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues values,
      MemorySegmentAccessInput input)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorerImpl(similarityFunction, values, input);
    }

    @Override
    public RandomVectorScorerSupplier copy() {
      return new RandomVectorScorerSupplierImpl(similarityFunction, values, input);
    }
  }

  private static class UpdateableRandomVectorScorerImpl extends RandomVectorScorerBase
      implements UpdateableRandomVectorScorer {
    private MemorySegment query;
    private OptimizedScalarQuantizer.QuantizationResult queryCorrectiveTerms;

    UpdateableRandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
        MemorySegmentAccessInput input)
        throws IOException {
      super(similarityFunction, values, input);
    }

    @Override
    public void setScoringOrdinal(int ord) throws IOException {
      checkOrdinal(ord);
      query = getVector(ord);
      queryCorrectiveTerms = getCorrectiveTerms(ord);
    }

    @Override
    public float score(int node) throws IOException {
      MemorySegment doc = getVector(node);
      float dotProduct =
          switch (getValues().getScalarEncoding()) {
            case UNSIGNED_BYTE -> PanamaVectorUtilSupport.uint8DotProduct(query, doc);
            case SEVEN_BIT -> PanamaVectorUtilSupport.dotProduct(query, doc);
            case PACKED_NIBBLE -> PanamaVectorUtilSupport.int4DotProductBothPacked(query, doc);
            case SINGLE_BIT_QUERY_NIBBLE ->
                throw new IllegalStateException(
                    "this should be handled by the default implementation");
          };
      // Call getCorrectiveTerms() after computing dot product since corrective terms
      // bytes appear after the vector bytes, so this sequence of calls is more cache
      // friendly.
      return Lucene104ScalarQuantizedVectorScorer.quantizedScore(
          getSimilarity(), getValues(), dotProduct, queryCorrectiveTerms, getCorrectiveTerms(node));
    }
  }
}
