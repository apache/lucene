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

import static org.apache.lucene.util.hnsw.RandomAccessVectorValues.fromByteVectorValues;
import static org.apache.lucene.util.hnsw.RandomAccessVectorValues.fromFloatVectorValues;

import java.io.IOException;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Computes the similarity score between a given query vector and different document vectors. This
 * is primarily used by {@link KnnFloatVectorQuery} to run an exact, exhaustive search over the
 * vectors.
 */
abstract class VectorScorer {

  /**
   * Create a new vector scorer instance.
   *
   * @param context the reader context
   * @param fi the FieldInfo for the field containing document vectors
   * @param query the query vector to compute the similarity for
   */
  static FloatVectorScorer create(LeafReaderContext context, FieldInfo fi, float[] query)
      throws IOException {
    FloatVectorValues values = context.reader().getFloatVectorValues(fi.name);
    if (values == null) {
      FloatVectorValues.checkField(context.reader(), fi.name);
      return null;
    }
    VectorSimilarity similarity = fi.getVectorSimilarity();
    VectorSimilarity.VectorScorer scorer =
        similarity.getVectorScorer(fromFloatVectorValues(values), query);
    return new FloatVectorScorer(values, scorer);
  }

  static ByteVectorScorer create(LeafReaderContext context, FieldInfo fi, byte[] query)
      throws IOException {
    ByteVectorValues values = context.reader().getByteVectorValues(fi.name);
    if (values == null) {
      ByteVectorValues.checkField(context.reader(), fi.name);
      return null;
    }
    VectorSimilarity similarity = fi.getVectorSimilarity();
    VectorSimilarity.VectorScorer scorer =
        similarity.getVectorScorer(fromByteVectorValues(values), query);
    return new ByteVectorScorer(values, scorer);
  }

  /** Compute the similarity score for the current document. */
  abstract float score() throws IOException;

  abstract boolean advanceExact(int doc) throws IOException;

  private static class ByteVectorScorer extends VectorScorer {
    private final ByteVectorValues values;
    private final VectorSimilarity.VectorScorer scorer;

    protected ByteVectorScorer(ByteVectorValues values, VectorSimilarity.VectorScorer scorer) {
      this.values = values;
      this.scorer = scorer;
    }

    /**
     * Advance the instance to the given document ID and return true if there is a value for that
     * document.
     */
    @Override
    public boolean advanceExact(int doc) throws IOException {
      int vectorDoc = values.docID();
      if (vectorDoc < doc) {
        vectorDoc = values.advance(doc);
      }
      return vectorDoc == doc;
    }

    @Override
    public float score() throws IOException {
      assert values.docID() != -1 : getClass().getSimpleName() + " is not positioned";
      return scorer.score(values.docID());
    }
  }

  private static class FloatVectorScorer extends VectorScorer {
    private final FloatVectorValues values;
    private final VectorSimilarity.VectorScorer scorer;

    protected FloatVectorScorer(FloatVectorValues values, VectorSimilarity.VectorScorer scorer) {
      this.values = values;
      this.scorer = scorer;
    }

    /**
     * Advance the instance to the given document ID and return true if there is a value for that
     * document.
     */
    @Override
    public boolean advanceExact(int doc) throws IOException {
      int vectorDoc = values.docID();
      if (vectorDoc < doc) {
        vectorDoc = values.advance(doc);
      }
      return vectorDoc == doc;
    }

    @Override
    public float score() throws IOException {
      assert values.docID() != -1 : getClass().getSimpleName() + " is not positioned";
      return scorer.score(values.docID());
    }
  }
}
