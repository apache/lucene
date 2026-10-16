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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Objects;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * For HNSW graph building over memory-mapped {@link ScalarEncoding#PACKED_NIBBLE} vectors, scores
 * each {@code bulkScore} batch with one call to {@link
 * NativeVectorUtilSupport#int4DotProductSinglePackedBulk}. Scores stay identical to scoring one
 * vector at a time.
 */
final class Lucene104NativeScalarQuantizedVectorScorer
    extends Lucene104ScalarQuantizedVectorScorer {

  // Each packed vector is followed by three float corrections and an int component sum.
  private static final int CORRECTIONS_BYTES = (Float.BYTES * 3) + Integer.BYTES;

  private static final ValueLayout.OfFloat LAYOUT_LE_FLOAT =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfInt LAYOUT_LE_INT =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  Lucene104NativeScalarQuantizedVectorScorer(FlatVectorsScorer nonQuantizedDelegate) {
    super(nonQuantizedDelegate);
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof OffHeapScalarQuantizedVectorValues values
        && values.getScalarEncoding() == ScalarEncoding.PACKED_NIBBLE
        && FilterIndexInput.unwrapOnlyTest(values.getSlice())
            instanceof MemorySegmentAccessInput input) {
      int stride = values.getVectorByteLength() + CORRECTIONS_BYTES;
      // Null when the vectors span more than one mapped chunk.
      MemorySegment vectors = input.segmentSliceOrNull(0, input.length());
      if (vectors != null && vectors.byteSize() == (long) stride * values.size()) {
        return new PackedNibbleScorerSupplier(values, similarityFunction, vectors);
      }
    }
    return super.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  private static final class PackedNibbleScorerSupplier implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityFunction similarity;
    private final MemorySegment vectors;

    PackedNibbleScorerSupplier(
        QuantizedByteVectorValues values,
        VectorSimilarityFunction similarity,
        MemorySegment vectors) {
      this.values = values;
      this.similarity = similarity;
      this.vectors = vectors;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new PackedNibbleScorer(values, similarity, vectors);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new PackedNibbleScorerSupplier(values.copy(), similarity, vectors);
    }
  }

  private static final class PackedNibbleScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityFunction similarity;
    private final MemorySegment vectors;
    private final int packedLength;
    private final int stride;
    private final byte[] query;
    private final MemorySegment querySegment;
    private QuantizationResult queryCorrections;
    private int[] ords;
    private int[] dotProducts;
    private MemorySegment ordsSegment;
    private MemorySegment dotProductsSegment;

    PackedNibbleScorer(
        QuantizedByteVectorValues values,
        VectorSimilarityFunction similarity,
        MemorySegment vectors) {
      super(values);
      this.values = values;
      this.similarity = similarity;
      this.vectors = vectors;
      this.packedLength = values.getVectorByteLength();
      this.stride = packedLength + CORRECTIONS_BYTES;
      this.query = new byte[2 * packedLength];
      this.querySegment = MemorySegment.ofArray(query);
      grow(16);
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      VectorUtil.int4Unpack(values.vectorValue(node), query);
      queryCorrections = values.getCorrectiveTerms(node);
    }

    @Override
    public float score(int node) throws IOException {
      int dotProduct = VectorUtil.int4DotProductSinglePacked(query, values.vectorValue(node));
      return quantizedScore(
          dotProduct, queryCorrections, values.getCorrectiveTerms(node), values, similarity);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      if (numNodes > ords.length) {
        grow(numNodes);
      }
      // First check are the ordinals in range.
      int size = maxOrd();
      for (int i = 0; i < numNodes; i++) {
        ords[i] = Objects.checkIndex(nodes[i], size);
      }
      NativeVectorUtilSupport.int4DotProductSinglePackedBulk(
          querySegment, vectors, stride, ordsSegment, numNodes, packedLength, dotProductsSegment);
      float max = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < numNodes; i++) {
        scores[i] =
            quantizedScore(
                dotProducts[i], queryCorrections, corrections(ords[i]), values, similarity);
        max = Math.max(max, scores[i]);
      }
      return max;
    }

    // Reads the mapped bytes, as values.getCorrectiveTerms(ord) overwrites the corrections cached
    // by the last values.vectorValue() call.
    private QuantizationResult corrections(int ord) {
      long offset = (long) ord * stride + packedLength;
      return new QuantizationResult(
          vectors.get(LAYOUT_LE_FLOAT, offset),
          vectors.get(LAYOUT_LE_FLOAT, offset + Float.BYTES),
          vectors.get(LAYOUT_LE_FLOAT, offset + 2 * Float.BYTES),
          vectors.get(LAYOUT_LE_INT, offset + 3 * Float.BYTES));
    }

    private void grow(int minSize) {
      ords = new int[ArrayUtil.oversize(minSize, Integer.BYTES)];
      dotProducts = new int[ords.length];
      ordsSegment = MemorySegment.ofArray(ords);
      dotProductsSegment = MemorySegment.ofArray(dotProducts);
    }
  }
}
