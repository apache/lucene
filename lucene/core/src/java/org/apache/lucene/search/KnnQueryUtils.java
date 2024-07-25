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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

/** Common utilities for KNN queries. */
class KnnQueryUtils {

  /** private constructor */
  private KnnQueryUtils() {}

  /**
   * Create a bit set for a set of matching docs which are also not deleted.
   *
   * <p>If there is no deleted doc, it will use the matching docs bit set. Otherwise, it will return
   * the bit set from matching docs which are also not deleted.
   *
   * @param iterator the matching doc iterator
   * @param liveDocs the segment live (non-deleted) doc
   * @param maxDoc the maximum number of docs to return
   * @return a bit set over the matching docs
   */
  static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return bitSetIterator.getBitSet();
    } else {
      // Create a new BitSet from matching and live docs
      FilteredDocIdSetIterator filterIterator =
          new FilteredDocIdSetIterator(iterator) {
            @Override
            protected boolean match(int doc) {
              return liveDocs == null || liveDocs.get(doc);
            }
          };
      return BitSet.of(filterIterator, maxDoc);
    }
  }

  /**
   * Create a Weight for the filtered query. The filter will also be enhanced to only match
   * documents with value in the KNN vector field.
   *
   * @param indexSearcher the index searcher to rewrite and create weight
   * @param filter the filter query
   * @param field the KNN vector field to check
   * @return Weight for the filter query
   */
  static Weight createFilterWeight(IndexSearcher indexSearcher, Query filter, String field)
      throws IOException {
    if (filter == null) {
      return null;
    }
    BooleanQuery booleanQuery =
        new BooleanQuery.Builder()
            .add(filter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
    Query rewritten = indexSearcher.rewrite(booleanQuery);
    return indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
  }
}
