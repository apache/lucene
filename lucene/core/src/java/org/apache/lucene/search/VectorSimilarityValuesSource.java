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
import org.apache.lucene.index.LeafReaderContext;

/**
 * An abstract class that provides the vector similarity scores between the query vector and the
 * {@link org.apache.lucene.document.KnnFloatVectorField} or {@link
 * org.apache.lucene.document.KnnByteVectorField} for documents.
 */
abstract class VectorSimilarityValuesSource extends DoubleValuesSource {
  protected final String fieldName;

  public VectorSimilarityValuesSource(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    VectorScorer scorer = getScorer(ctx);
    if (scorer == null) {
      return DoubleValues.EMPTY;
    }
    DocIdSetIterator iterator = scorer.iterator();
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return scorer.score();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return doc >= iterator.docID() && (iterator.docID() == doc || iterator.advance(doc) == doc);
      }
    };
  }

  protected abstract VectorScorer getScorer(LeafReaderContext ctx) throws IOException;

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
