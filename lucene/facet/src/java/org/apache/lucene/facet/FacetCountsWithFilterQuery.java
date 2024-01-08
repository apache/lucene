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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Base class for facet counts. It allows for a query to be passed in to filter the match set.
 *
 * @lucene.experimental
 */
public abstract class FacetCountsWithFilterQuery extends Facets {

  /**
   * Optional: if specified, we first test this Query to see whether the document should be checked
   * for matching ranges. If this is null, all documents are checked.
   */
  protected final Query fastMatchQuery;

  /** Create {@code FacetCounts} */
  protected FacetCountsWithFilterQuery(Query fastMatchQuery) {
    this.fastMatchQuery = fastMatchQuery;
  }

  /**
   * Create a {@link DocIdSetIterator} from the provided {@code hits} that relies on {@code
   * fastMatchQuery} if available for first-pass filtering. If {@code iterators} is not empty then
   * all iterators are intersected. If any of the iterators is null, it indicates no documents will
   * be matched by it, and therefore no documents will be matched overall. A null response indicates
   * no documents will match.
   */
  protected DocIdSetIterator createIterator(
      FacetsCollector.MatchingDocs hits, DocIdSetIterator... iterators) throws IOException {
    List<DocIdSetIterator> allIterators = new ArrayList<>();
    allIterators.add(hits.bits.iterator());
    allIterators.addAll(Arrays.asList(iterators));
    if (allIterators.stream().anyMatch(Objects::isNull)) {
      // if any of the iterators are null, there are no matching docs
      return null;
    }

    if (fastMatchQuery != null) {
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(hits.context);
      final IndexSearcher searcher = new IndexSearcher(topLevelContext);
      searcher.setQueryCache(null);
      final Weight fastMatchWeight =
          searcher.createWeight(searcher.rewrite(fastMatchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
      final Scorer s = fastMatchWeight.scorer(hits.context);
      if (s == null) {
        // no matching docs by the fast match query
        return null;
      } else {
        DocIdSetIterator fastMatchQueryIterator = s.iterator();
        allIterators.add(fastMatchQueryIterator);
      }
    }

    if (allIterators.size() == 1) {
      return allIterators.get(0);
    } else {
      return ConjunctionUtils.intersectIterators(allIterators);
    }
  }
}
