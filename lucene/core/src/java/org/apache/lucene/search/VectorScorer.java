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
   * An optional bulk scorer implementation that allows bulk scoring over the provided matching
   * docs. The iterator of this instance of VectorScorer should be used and iterated in conjunction
   * with the provided matchingDocs iterator to score only the documents that are present in both
   * iterators. If the provided matchingDocs iterator is null, then all documents should be scored.
   * Additionally, if the iterators are unpositioned (docID() == -1), this method should position
   * them to the first document.
   *
   * @param matchingDocs the documents to score
   * @return a {@link Bulk} scorer
   * @throws IOException if an exception occurs during bulk scorer creation
   * @lucene.experimental
   */
  default Bulk bulk(DocIdSetIterator matchingDocs) throws IOException {
    final DocIdSetIterator iterator =
        matchingDocs == null
            ? iterator()
            : ConjunctionUtils.createConjunction(List.of(matchingDocs, iterator()), List.of());
    if (iterator.docID() == -1) {
      iterator.nextDoc();
    }
    return (nextCount, liveDocs, buffer) -> {
      buffer.growNoCopy(nextCount);
      int size = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      for (int doc = iterator.docID();
          doc != DocIdSetIterator.NO_MORE_DOCS && size < nextCount;
          doc = iterator.nextDoc()) {
        if (liveDocs == null || liveDocs.get(doc)) {
          buffer.docs[size] = doc;
          buffer.features[size] = score();
          maxScore = Math.max(maxScore, buffer.features[size]);
          ++size;
        }
      }
      buffer.size = size;
      return maxScore;
    };
  }

  /**
   * Bulk scorer interface to score multiple vectors at once
   *
   * @lucene.experimental
   */
  interface Bulk {
    /**
     * Score up to nextCount documents, store the results in the provided buffer. Behaves similarly
     * to {@link Scorer#nextDocsAndScores(int, Bits, DocAndFloatFeatureBuffer)}
     *
     * @param nextCount the maximum number of documents to score
     * @param liveDocs the live docs, or null if all docs are live
     * @param buffer the buffer to store the results
     * @return the max score of the scored documents
     * @throws IOException if an exception occurs during scoring
     */
    float nextDocsAndScores(int nextCount, Bits liveDocs, DocAndFloatFeatureBuffer buffer)
        throws IOException;
  }
}
