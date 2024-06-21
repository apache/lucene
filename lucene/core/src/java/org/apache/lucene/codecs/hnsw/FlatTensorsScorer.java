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

import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;
import java.util.List;

/**
 * Provides mechanisms to score tensors that are stored in a flat file The purpose of this class is
 * for providing flexibility to the codec utilizing the tensors
 *
 * @lucene.experimental
 */
public interface FlatTensorsScorer {

  /**
   * Returns a {@link RandomVectorScorerSupplier} that can be used to score tensor values
   *
   * @param similarityFunction the similarity function to use
   * @param values the tensor values to score
   * @return a {@link RandomVectorScorerSupplier} that can be used to score tensors
   * @throws IOException if an I/O error occurs
   */
  RandomVectorScorerSupplier getRandomTensorScorerSupplier(
      TensorSimilarityFunction similarityFunction, RandomAccessVectorValues values)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given set of tensors and target tensor.
   *
   * @param similarityFunction the similarity function to use
   * @param values the tensor values to score
   * @param target the target tensor with vector values packed in a single array
   * @return a {@link RandomVectorScorer} for the given field and target tensor.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  RandomVectorScorer getRandomTensorScorer(
      TensorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      float[] target)
      throws IOException;

  /**
   * Returns a {@link RandomVectorScorer} for the given set of tensors and target tensor.
   *
   * @param similarityFunction the similarity function to use
   * @param values the tensor values to score
   * @param target the target tensor with vector values packed in a single array
   * @return a {@link RandomVectorScorer} for the given field and target tensor.
   * @throws IOException if an I/O error occurs when reading from the index.
   */
  RandomVectorScorer getRandomTensorScorer(
      TensorSimilarityFunction similarityFunction,
      RandomAccessVectorValues values,
      byte[] target)
      throws IOException;
}
