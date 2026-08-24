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
package org.apache.lucene.sandbox.codecs.dedup;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.dedup.DedupScalarQuantizedVectorValues.FieldValues;
import org.apache.lucene.sandbox.codecs.dedup.DedupScalarQuantizedVectorValues.RawAndQuantizedValues;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

/**
 * Scorer for scalar quantized, de-duplicated vectors: a {@link DedupFlatVectorsScorer} delegating
 * vector operations to the {@link Lucene104ScalarQuantizedVectorScorer}, whose record and scoring
 * conventions the quantized group views follow exactly (see {@link DedupQuantizer.Flavor}).
 *
 * @lucene.experimental
 */
final class DedupScalarQuantizedVectorsScorer extends DedupFlatVectorsScorer {

  private static final Lucene104ScalarQuantizedVectorScorer QUANTIZED_SCORER =
      new Lucene104ScalarQuantizedVectorScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  DedupScalarQuantizedVectorsScorer() {
    super(QUANTIZED_SCORER);
  }

  /** Full-precision views resolve to their quantized values for scoring. */
  @Override
  protected KnnVectorValues unwrap(KnnVectorValues vectorValues) {
    if (vectorValues instanceof RawAndQuantizedValues rawAndQuantized) {
      return rawAndQuantized.getQuantizedValues();
    }
    return vectorValues;
  }

  /**
   * Supplier for merge-time graph construction with asymmetric encodings: query-encoded distinct
   * vectors (one per <b>group</b> ordinal, in a temporary file) are compared against the stored
   * doc-encoded vectors, both sides resolving through {@code fieldOrdToGroupOrd} (see {@link
   * DedupScalarQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge}).
   */
  static RandomVectorScorerSupplier asymmetricMergeSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues queryValues,
      FieldValues targetValues) {
    RandomVectorScorerSupplier groupView =
        QUANTIZED_SCORER.getRandomVectorScorerSupplier(
            similarityFunction, queryValues, targetValues.getGroupView());
    return new RandomVectorScorerSupplierImpl(
        targetValues, groupView, targetValues.getFieldOrdToGroupOrd());
  }
}
