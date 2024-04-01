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

package org.apache.lucene.util.hnsw;

import java.io.IOException;
import org.apache.lucene.codecs.ByteVectorProvider;
import org.apache.lucene.codecs.FloatVectorProvider;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.index.VectorSimilarityFunction;

/** A supplier that creates {@link RandomVectorScorer} from an ordinal. */
public interface RandomVectorScorerSupplier {
  /**
   * This creates a {@link RandomVectorScorer} for scoring random nodes in batches against the given
   * ordinal.
   *
   * @param ord the ordinal of the node to compare
   * @return a new {@link RandomVectorScorer}
   */
  RandomVectorScorer scorer(int ord) throws IOException;

  /**
   * Make a copy of the supplier, which will copy the underlying vectorValues so the copy is safe to
   * be used in other threads.
   */
  RandomVectorScorerSupplier copy() throws IOException;

  /**
   * Creates a {@link RandomVectorScorerSupplier} to compare float vectors. The vectorValues passed
   * in will be copied and the original copy will not be used.
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   */
  static RandomVectorScorerSupplier createFloats(
      final RandomAccessVectorValues<float[]> vectors, final VectorSimilarity similarityFunction)
      throws IOException {
    // We copy the provided random accessor just once during the supplier's initialization
    // and then reuse it consistently across all scorers for conducting vector comparisons.
    return new FloatScoringSupplier(vectors, similarityFunction);
  }

  /** See {@link #createFloats(RandomAccessVectorValues, VectorSimilarity)} */
  @Deprecated
  static RandomVectorScorerSupplier createFloats(
      final RandomAccessVectorValues<float[]> vectors,
      final VectorSimilarityFunction similarityFunction)
      throws IOException {
    return new FloatScoringSupplier(
        vectors, VectorSimilarity.fromVectorSimilarityFunction(similarityFunction));
  }

  /**
   * Creates a {@link RandomVectorScorerSupplier} to compare byte vectors. The vectorValues passed
   * in will be copied and the original copy will not be used.
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   */
  static RandomVectorScorerSupplier createBytes(
      final RandomAccessVectorValues<byte[]> vectors, final VectorSimilarity similarityFunction)
      throws IOException {
    // We copy the provided random accessor only during the supplier's initialization
    // and then reuse it consistently across all scorers for conducting vector comparisons.
    return new ByteScoringSupplier(vectors, similarityFunction);
  }

  /** See {@link #createBytes(RandomAccessVectorValues, VectorSimilarity)} */
  @Deprecated
  static RandomVectorScorerSupplier createBytes(
      final RandomAccessVectorValues<byte[]> vectors,
      final VectorSimilarityFunction similarityFunction)
      throws IOException {
    return new ByteScoringSupplier(
        vectors, VectorSimilarity.fromVectorSimilarityFunction(similarityFunction));
  }

  /** RandomVectorScorerSupplier for bytes vector */
  final class ByteScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues<byte[]> vectors;
    private final ByteVectorProvider byteVectorProvider;
    private final VectorSimilarity similarityFunction;

    private ByteScoringSupplier(
        RandomAccessVectorValues<byte[]> vectors, VectorSimilarity similarityFunction)
        throws IOException {
      this.vectors = vectors;
      this.byteVectorProvider = ByteVectorProvider.fromRandomAccessVectorValues(vectors.copy());
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      VectorSimilarity.VectorScorer scorer =
          similarityFunction.getByteVectorComparator(byteVectorProvider).asScorer(ord);
      return new RandomVectorScorer(vectors, scorer);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ByteScoringSupplier(vectors, similarityFunction);
    }
  }

  /** RandomVectorScorerSupplier for Float vector */
  final class FloatScoringSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessVectorValues<float[]> vectors;
    private final FloatVectorProvider floatVectorProvider;
    private final VectorSimilarity similarityFunction;

    private FloatScoringSupplier(
        RandomAccessVectorValues<float[]> vectors, VectorSimilarity similarityFunction)
        throws IOException {
      this.vectors = vectors;
      this.floatVectorProvider = FloatVectorProvider.fromRandomAccessVectorValues(vectors.copy());
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      VectorSimilarity.VectorScorer scorer =
          similarityFunction.getFloatVectorComparator(floatVectorProvider).asScorer(ord);
      return new RandomVectorScorer(vectors, scorer);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new FloatScoringSupplier(vectors, similarityFunction);
    }
  }
}
