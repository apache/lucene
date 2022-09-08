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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;

/**
 * Computes the similarity score between a given query vector and different document vectors. This
 * is primarily used by {@link org.apache.lucene.search.KnnVectorQuery} to run an exact, exhaustive
 * search over the vectors.
 */
abstract class VectorScorer {
  protected final VectorValues values;
  protected final VectorSimilarityFunction similarity;

  /**
   * Create a new vector scorer instance.
   *
   * @param context the reader context
   * @param fi the FieldInfo for the field containing document vectors
   * @param query the query vector to compute the similarity for
   */
  static VectorScorer create(LeafReaderContext context, FieldInfo fi, float[] query)
      throws IOException {
    VectorValues values = context.reader().getVectorValues(fi.name);
    VectorSimilarityFunction similarity = fi.getVectorSimilarityFunction();
    switch (fi.getVectorEncoding()) {
      case BYTE:
        return new ByteVectorScorer(values, query, similarity);
      default:
      case FLOAT32:
        return new FloatVectorScorer(values, query, similarity);
    }
  }

  VectorScorer(VectorValues values, VectorSimilarityFunction similarity) {
    this.values = values;
    this.similarity = similarity;
  }

  /**
   * Advance the instance to the given document ID and return true if there is a value for that
   * document.
   */
  public boolean advanceExact(int doc) throws IOException {
    int vectorDoc = values.docID();
    if (vectorDoc < doc) {
      vectorDoc = values.advance(doc);
    }
    return vectorDoc == doc;
  }

  /** Compute the similarity score for the current document. */
  abstract float score() throws IOException;

  private static class ByteVectorScorer extends VectorScorer {
    private final BytesRef query;

    protected ByteVectorScorer(
        VectorValues values, float[] query, VectorSimilarityFunction similarity) {
      super(values, similarity);
      this.query = VectorUtil.toBytesRef(query);
    }

    @Override
    public float score() throws IOException {
      return similarity.compare(query, values.binaryValue());
    }
  }

  private static class FloatVectorScorer extends VectorScorer {
    private final float[] query;

    protected FloatVectorScorer(
        VectorValues values, float[] query, VectorSimilarityFunction similarity) {
      super(values, similarity);
      this.query = query;
    }

    @Override
    public float score() throws IOException {
      return similarity.compare(query, values.vectorValue());
    }
  }
}
