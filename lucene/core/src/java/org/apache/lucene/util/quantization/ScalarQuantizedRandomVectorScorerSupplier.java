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
package org.apache.lucene.util.quantization;

import java.io.IOException;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Quantized vector scorer supplier
 *
 * @lucene.experimental
 */
public class ScalarQuantizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {

  private final QuantizedByteVectorProvider values;
  private final ScalarQuantizedVectorSimilarity similarity;

  public ScalarQuantizedRandomVectorScorerSupplier(
      VectorSimilarity similarityFunction,
      ScalarQuantizer scalarQuantizer,
      QuantizedByteVectorProvider values) {
    this.similarity =
        new ScalarQuantizedVectorSimilarity(
            similarityFunction, scalarQuantizer.getConstantMultiplier(), scalarQuantizer.getBits());
    this.values = values;
  }

  private ScalarQuantizedRandomVectorScorerSupplier(
      ScalarQuantizedVectorSimilarity similarity, QuantizedByteVectorProvider values) {
    this.similarity = similarity;
    this.values = values;
  }

  @Override
  public RandomVectorScorer scorer(int ord) throws IOException {
    VectorSimilarity.VectorComparator comparator = similarity.getByteVectorComparator(values);
    return new RandomVectorScorer(values, comparator.asScorer(ord));
  }

  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    return new ScalarQuantizedRandomVectorScorerSupplier(similarity, values.copy());
  }
}
