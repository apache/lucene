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

package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Provides mechanisms to score vectors that are stored in a flat file The purpose of this class is
 * for providing flexibility to the codec utilizing the vectors
 *
 * @lucene.experimental
 */
public interface FlatVectorsScorer {

  /**
   * Returns a {@link RandomVectorScorerSupplier} that can be used to score vectors
   *
   * @param similarityFunction the similarity function to use
   * @param vectorValues the vector values to score
   * @return a {@link RandomVectorScorerSupplier} that can be used to score vectors
   * @throws IOException if an I/O error occurs
   */
  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given set of vectors and target vector.
   *
   * @param similarityFunction the similarity function to use
   * @param vectorValues the vector values to score
   * @param target the target vector
   * @return a {@link RandomVectorScorer} for the given field and target vector.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given set of vectors and target vector.
   *
   * @param similarityFunction the similarity function to use
   * @param vectorValues the vector values to score
   * @param target the target vector
   * @return a {@link RandomVectorScorer} for the given field and target vector.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException;
}
