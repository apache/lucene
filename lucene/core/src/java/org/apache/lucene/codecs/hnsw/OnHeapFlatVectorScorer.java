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

/** On-heap implementation of {@link FlatVectorsScorer}. */
public class OnHeapFlatVectorScorer implements FlatVectorsScorer {
  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof RandomAccessVectorValues.Floats floatVectorValues) {
      return RandomVectorScorerSupplier.createFloats(floatVectorValues, similarityFunction);
    } else if (vectorValues instanceof RandomAccessVectorValues.Bytes byteVectorValues) {
      return RandomVectorScorerSupplier.createBytes(byteVectorValues, similarityFunction);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Floats or RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Floats;
    return RandomVectorScorer.createFloats(
        (RandomAccessVectorValues.Floats) vectorValues, similarityFunction, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    return RandomVectorScorer.createBytes(
        (RandomAccessVectorValues.Bytes) vectorValues, similarityFunction, target);
  }
}
