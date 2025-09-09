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
import java.util.List;
import org.apache.lucene.util.Bits;

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

  /**
   * An optional bulk scorer implementation that allows bulk scoring over the provided matching docs
   */
  default Bulk bulk(DocIdSetIterator matchingDocs) {
    final DocIdSetIterator iterator =
        ConjunctionUtils.createConjunction(List.of(matchingDocs, iterator()), List.of());
    return (nextCount, liveDocs, buffer) -> {
      buffer.growNoCopy(nextCount);
      int size = 0;
      for (int doc = iterator.docID();
          doc != DocIdSetIterator.NO_MORE_DOCS && size < nextCount;
          doc = iterator.nextDoc()) {
        if (liveDocs == null || liveDocs.get(doc)) {
          buffer.docs[size] = doc;
          buffer.features[size] = score();
          ++size;
        }
      }
      buffer.size = size;
      if (liveDocs != null) {
        buffer.apply(liveDocs);
      }
    };
  }

  interface Bulk {
    void nextDocsAndScores(int nextCount, Bits liveDocs, DocAndFloatFeatureBuffer buffer)
        throws IOException;
  }
}
