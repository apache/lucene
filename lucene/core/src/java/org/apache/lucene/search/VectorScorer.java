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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Computes the similarity score between a given query vector and different document vectors. This
 * is used for exact searching and scoring
 *
 * @lucene.experimental
 */
public interface VectorScorer {

  /**
   * Compute the score for the current document ID.
   *
   * @return the score for the current document ID
   * @throws IOException if an exception occurs during score computation
   */
  float score() throws IOException;

  /**
   * @return a {@link DocIdSetIterator} over the documents.
   */
  DocIdSetIterator iterator();

  class ByteVectorScorer implements VectorScorer {
    private final byte[] query;
    private final ByteVectorValues values;
    private final VectorSimilarityFunction similarity;

    protected ByteVectorScorer(
        ByteVectorValues values, byte[] query, VectorSimilarityFunction similarity) {
      this.similarity = similarity;
      this.values = values;
      this.query = query;
    }

    /**
     * Advance the instance to the given document ID and return true if there is a value for that
     * document.
     */
    @Override
    public DocIdSetIterator iterator() {
      return values;
    }

    @Override
    public float score() throws IOException {
      assert values.docID() != -1 : getClass().getSimpleName() + " is not positioned";
      return similarity.compare(query, values.vectorValue());
    }
  }

  class FloatVectorScorer implements VectorScorer {
    private final float[] query;
    private final FloatVectorValues values;
    private final VectorSimilarityFunction similarity;

    protected FloatVectorScorer(
        FloatVectorValues values, float[] query, VectorSimilarityFunction similarity) {
      this.similarity = similarity;
      this.query = query;
      this.values = values;
    }

    @Override
    public DocIdSetIterator iterator() {
      return values;
    }

    @Override
    public float score() throws IOException {
      assert values.docID() != -1 : getClass().getSimpleName() + " is not positioned";
      return similarity.compare(query, values.vectorValue());
    }
  }
}
