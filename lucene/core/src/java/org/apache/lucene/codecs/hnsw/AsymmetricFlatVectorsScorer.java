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
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

/**
 * A {@link FlatVectorsScorer} of quantized vectors whose query side may be quantized differently
 * from the stored side, so that one set of vectors can be scored against another, for example when
 * merging.
 *
 * @lucene.experimental
 */
public interface AsymmetricFlatVectorsScorer extends FlatVectorsScorer {

  /**
   * Returns a supplier of scorers of the vectors of {@code scoringVectors}, quantized like the
   * query side of the encoding of {@code targetVectors}, against {@code targetVectors}.
   *
   * @throws IOException if the vectors cannot be read
   */
  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues scoringVectors,
      QuantizedByteVectorValues targetVectors)
      throws IOException;
}
