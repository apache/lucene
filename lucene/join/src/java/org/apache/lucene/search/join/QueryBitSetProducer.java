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
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;

/** A {@link BitSetProducer} that wraps a query and caches matching {@link BitSet}s per segment. */
public class QueryBitSetProducer implements BitSetProducer {
  private final Query query;
  final Map<IndexReader.CacheKey, BitSet> cache = Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Wraps another query's result and caches it into bitsets.
   *
   * @param query Query to cache results of
   */
  public QueryBitSetProducer(Query query) {
    this.query = query;
  }

  /**
   * Gets the contained query.
   *
   * @return the contained query.
   */
  public Query getQuery() {
    return query;
  }

  private static final BitSet SENTINEL = new FixedBitSet(0);

  @Override
  public BitSet getBitSet(LeafReaderContext context) throws IOException {
    final LeafReader reader = context.reader();
    final IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();

    BitSet bitSet = null;
    if (cacheHelper != null) {
      bitSet = cache.get(cacheHelper.getKey());
    }
    if (bitSet == null) {
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      final IndexSearcher searcher = new IndexSearcher(topLevelContext);
      searcher.setQueryCache(null);
      final Query rewritten = searcher.rewrite(query);
      final Weight weight =
          searcher.createWeight(
              rewritten, org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1);
      final Scorer s = weight.scorer(context);

      if (s == null) {
        bitSet = SENTINEL;
      } else {
        bitSet = BitSet.of(s.iterator(), context.reader().maxDoc());
      }
      if (cacheHelper != null) {
        cache.put(cacheHelper.getKey(), bitSet);
      }
    }
    return bitSet == SENTINEL ? null : bitSet;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + query.toString() + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryBitSetProducer other = (QueryBitSetProducer) o;
    return this.query.equals(other.query);
  }

  @Override
  public int hashCode() {
    return 31 * getClass().hashCode() + query.hashCode();
  }
}
