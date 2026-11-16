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

package org.apache.lucene.monitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

abstract class QueryIndex implements Closeable {
  static final class FIELDS {
    static final String query_id = "_query_id";
    static final String cache_id = "_cache_id";
    static final String mq = "_mq";
  }

  protected SearcherManager manager;
  protected QueryDecomposer decomposer;
  protected MonitorQuerySerializer serializer;

  // package-private for testing
  final Map<IndexReader.CacheKey, QueryTermFilter> termFilters = new ConcurrentHashMap<>();

  protected final List<MonitorUpdateListener> listeners = new ArrayList<>();

  abstract void commit(List<MonitorQuery> updates) throws IOException;

  public MonitorQuery getQuery(String queryId) throws IOException {
    if (serializer == null) {
      throw new IllegalStateException(
          "Cannot get queries from an index with no MonitorQuerySerializer");
    }
    AtomicReference<BytesRef> bytesHolder = new AtomicReference<>();
    search(
        new TermQuery(new Term(FIELDS.query_id, queryId)),
        (_, _, dataValues) -> bytesHolder.set(BytesRef.deepCopyOf(dataValues.mq.binaryValue())));
    return bytesHolder.get() != null ? serializer.deserialize(bytesHolder.get()) : null;
  }

  /**
   * Calls the collector for every query in the index. The collector may be called from multiple
   * threads at once.
   */
  public void scan(QueryCollector collector) throws IOException {
    search(MatchAllDocsQuery.INSTANCE, collector);
  }

  /** Searches the index with a single collector, which may be called from multiple threads. */
  long search(final Query query, QueryCollector collector) throws IOException {
    QueryBuilder builder = _ -> query;
    return search(
        builder,
        new QueryCollectorManager<>() {
          @Override
          public QueryCollector newCollector() {
            return collector;
          }

          @Override
          public void reduce(List<QueryCollector> collectors) {}
        });
  }

  /**
   * Searches the index, using a separate collector for each slice of the index that is searched.
   *
   * @return how long it took to build the query, in nanoseconds
   */
  abstract <C extends QueryCollector> long search(
      QueryBuilder queryBuilder, QueryCollectorManager<C> collectorManager) throws IOException;

  public abstract void purgeCache() throws IOException;

  abstract void purgeCache(CachePopulator populator) throws IOException;

  abstract int numDocs() throws IOException;

  public abstract int cacheSize();

  abstract void deleteQueries(List<String> ids) throws IOException;

  abstract void clear() throws IOException;

  public abstract long getLastPurged();

  public void addListener(MonitorUpdateListener listener) {
    listeners.add(listener);
  }

  public interface QueryCollector {

    void matchQuery(String id, QueryCacheEntry query, DataValues dataValues) throws IOException;

    default ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  /**
   * Creates a {@link QueryCollector} for each slice of the index that is searched, and combines
   * their results once the search completes.
   *
   * <p>Like {@link CollectorManager}, but for the {@link QueryCollector}s that the index collectors
   * wrap.
   */
  interface QueryCollectorManager<C extends QueryCollector> {

    C newCollector() throws IOException;

    void reduce(List<C> collectors) throws IOException;
  }

  /** Adapts a {@link QueryCollectorManager} to a Lucene {@link CollectorManager}. */
  static <C extends QueryCollector, L extends Collector> CollectorManager<L, Void> collectorManager(
      QueryCollectorManager<C> queryCollectorManager,
      Function<C, L> toLuceneCollector,
      Function<L, C> toQueryCollector) {
    return new CollectorManager<>() {
      @Override
      public L newCollector() throws IOException {
        return toLuceneCollector.apply(queryCollectorManager.newCollector());
      }

      @Override
      public Void reduce(Collection<L> collectors) throws IOException {
        List<C> queryCollectors = new ArrayList<>(collectors.size());
        for (L collector : collectors) {
          queryCollectors.add(toQueryCollector.apply(collector));
        }
        queryCollectorManager.reduce(queryCollectors);
        return null;
      }
    };
  }

  interface QueryBuilder {
    Query buildQuery(BiPredicate<String, BytesRef> termAcceptor) throws IOException;
  }

  interface CachePopulator {
    void populateCacheWithIndex(Map<String, QueryCacheEntry> newCache) throws IOException;
  }

  public static final class DataValues {
    SortedDocValues queryId;
    SortedDocValues cacheId;
    BinaryDocValues mq;
    Weight weight;
    LeafReaderContext ctx;
    int docID;

    void advanceTo(int doc) throws IOException {
      queryId.advanceExact(doc);
      cacheId.advanceExact(doc);
      if (mq != null) {
        mq.advanceExact(doc);
      }
      this.docID = doc;
    }
  }

  static class QueryTermFilter implements BiPredicate<String, BytesRef> {

    private final Map<String, BytesRefHash> termsHash = new HashMap<>();

    QueryTermFilter(IndexReader reader) throws IOException {
      for (LeafReaderContext ctx : reader.leaves()) {
        for (FieldInfo fi : ctx.reader().getFieldInfos()) {
          BytesRefHash terms = termsHash.computeIfAbsent(fi.name, _ -> new BytesRefHash());
          Terms t = ctx.reader().terms(fi.name);
          if (t != null) {
            TermsEnum te = t.iterator();
            BytesRef term;
            while ((term = te.next()) != null) {
              terms.add(term);
            }
          }
        }
      }
    }

    @Override
    public boolean test(String field, BytesRef term) {
      BytesRefHash bytes = termsHash.get(field);
      if (bytes == null) {
        return false;
      }
      return bytes.find(term) != -1;
    }
  }
}
