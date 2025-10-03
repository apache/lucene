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

package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

public interface AsymmetricScalarQuantizeFlatVectorsScorer extends FlatVectorsScorer {
  /**
   * Returns a {@link RandomVectorScorerSupplier} that can be used to score asymmetric vector
   * representations, typically a higher fidelity "scoring" vector against a lower fidelity "target"
   * vector. This is used during indexing to improve the quality of the index data structure during
   * build/merge; only the targetVectors are saved.
   *
   * <p>This may only be used when ScalarEncoding.isAsymmetric().
   *
   * @param similarityFunction the similarity function to use
   * @param scoringVectors higher fidelity scoring vectors to use as queries.
   * @param targetVectors lower fidelity vectors to use as documents.
   * @return a {@link RandomVectorScorerSupplier} that can be used to score vectors
   * @throws IOException if an I/O error occurs
   */
  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues scoringVectors,
      QuantizedByteVectorValues targetVectors)
      throws IOException;
}
