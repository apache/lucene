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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * kNN byte vector query that joins matching children vector documents with their parent doc id. The
 * top documents returned are the child document ids and the calculated scores. Here is how to use
 * this in conjunction with {@link ToParentBlockJoinQuery}.
 *
 * <pre class="prettyprint">
 *   Query knnQuery = new DiversifyingChildrenByteKnnVectorQuery(fieldName, queryVector, ...);
 *   // Rewrite executes kNN search and collects nearest children docIds and their scores
 *   Query rewrittenKnnQuery = searcher.rewrite(knnQuery);
 *   // Join the scored children docs with their parents and score the parents
 *   Query childrenToParents = new ToParentBlockJoinQuery(rewrittenKnnQuery, parentsFilter, ScoreMode.MAX);
 * </pre>
 */
public class DiversifyingChildrenByteKnnVectorQuery extends KnnByteVectorQuery {
  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

  private final BitSetProducer parentsFilter;
  private final Query childFilter;
  private final int k;
  private final byte[] query;

  /**
   * Create a ToParentBlockJoinByteVectorQuery.
   *
   * @param field the query field
   * @param query the vector query
   * @param childFilter the child filter
   * @param k how many parent documents to return given the matching children
   * @param parentsFilter Filter identifying the parent documents.
   */
  public DiversifyingChildrenByteKnnVectorQuery(
      String field, byte[] query, Query childFilter, int k, BitSetProducer parentsFilter) {
    super(field, query, k, childFilter);
    this.childFilter = childFilter;
    this.parentsFilter = parentsFilter;
    this.k = k;
    this.query = query;
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new DiversifyingNearestChildrenKnnCollectorManager(k, parentsFilter);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return NO_RESULTS;
    }
    KnnCollector collector =
        new DiversifyingNearestChildrenKnnCollector(k, visitedLimit, parentBitSet);
    context.reader().searchNearestVectors(field, query, collector, acceptDocs);
    return collector.topDocs();
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + ":" + this.field + "[" + query[0] + ",...][" + k + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DiversifyingChildrenByteKnnVectorQuery that = (DiversifyingChildrenByteKnnVectorQuery) o;
    return k == that.k
        && Objects.equals(parentsFilter, that.parentsFilter)
        && Objects.equals(childFilter, that.childFilter)
        && Arrays.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), parentsFilter, childFilter, k);
    result = 31 * result + Arrays.hashCode(query);
    return result;
  }
}
