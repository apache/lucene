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
   * Creates a {@link RandomVectorScorerSupplier} to compare float vectors.
   *
   * <p>WARNING: The {@link RandomAccessVectorValues} given can contain stateful buffers. Avoid
   * using it after calling this function. If you plan to use it again outside the returned {@link
   * RandomVectorScorer}, think about passing a copied version ({@link
   * RandomAccessVectorValues#copy}).
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   */
  static RandomVectorScorerSupplier createFloats(
      final RandomAccessVectorValues<float[]> vectors,
      final VectorSimilarityFunction similarityFunction)
      throws IOException {
    // We copy the provided random accessor just once during the supplier's initialization
    // and then reuse it consistently across all scorers for conducting vector comparisons.
    final RandomAccessVectorValues<float[]> vectorsCopy = vectors.copy();
    return queryOrd ->
        (RandomVectorScorer)
            cand ->
                similarityFunction.compare(
                    vectors.vectorValue(queryOrd), vectorsCopy.vectorValue(cand));
  }

  /**
   * Creates a {@link RandomVectorScorerSupplier} to compare byte vectors.
   *
   * <p>WARNING: The {@link RandomAccessVectorValues} given can contain stateful buffers. Avoid
   * using it after calling this function. If you plan to use it again outside the returned {@link
   * RandomVectorScorer}, think about passing a copied version ({@link
   * RandomAccessVectorValues#copy}).
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   */
  static RandomVectorScorerSupplier createBytes(
      final RandomAccessVectorValues<byte[]> vectors,
      final VectorSimilarityFunction similarityFunction)
      throws IOException {
    // We copy the provided random accessor just once during the supplier's initialization
    // and then reuse it consistently across all scorers for conducting vector comparisons.
    final RandomAccessVectorValues<byte[]> vectorsCopy = vectors.copy();
    return queryOrd ->
        (RandomVectorScorer)
            cand ->
                similarityFunction.compare(
                    vectors.vectorValue(queryOrd), vectorsCopy.vectorValue(cand));
  }
}
