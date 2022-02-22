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
import java.util.*;
import java.util.function.BiPredicate;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
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
  protected long lastPurged = -1;

  /* The current query cache */
  protected volatile Map<String, QueryCacheEntry> queries;
  // NB this is not final because it can be replaced by purgeCache()

  // package-private for testing
  final Map<IndexReader.CacheKey, QueryTermFilter> termFilters = new HashMap<>();

  abstract void commit(List<MonitorQuery> updates) throws IOException;

  public MonitorQuery getQuery(String queryId) throws IOException {
    if (serializer == null) {
      throw new IllegalStateException(
          "Cannot get queries from an index with no MonitorQuerySerializer");
    }
    BytesRef[] bytesHolder = new BytesRef[1];
    search(
        new TermQuery(new Term(FIELDS.query_id, queryId)),
        (id, query, dataValues) -> bytesHolder[0] = dataValues.mq.binaryValue());
    return serializer.deserialize(bytesHolder[0]);
  }

  protected void populateQueryCache(MonitorQuerySerializer serializer, QueryDecomposer decomposer)
      throws IOException {
    if (serializer == null) {
      // No query serialization happening here - check that the cache is empty
      IndexSearcher searcher = manager.acquire();
      try {
        if (searcher.count(new MatchAllDocsQuery()) != 0) {
          throw new IllegalStateException(
              "Attempting to open a non-empty monitor query index with no MonitorQuerySerializer");
        }
      } finally {
        manager.release(searcher);
      }
      return;
    }
    Set<String> ids = new HashSet<>();
    List<Exception> errors = new ArrayList<>();
    purgeCache(
        newCache ->
            scan(
                (id, cacheEntry, dataValues) -> {
                  if (ids.contains(id)) {
                    // this is a branch of a query that has already been reconstructed, but
                    // then split by decomposition - we don't need to parse it again
                    return;
                  }
                  ids.add(id);
                  try {
                    MonitorQuery mq = serializer.deserialize(dataValues.mq.binaryValue());
                    for (QueryCacheEntry entry : QueryCacheEntry.decompose(mq, decomposer)) {
                      newCache.put(entry.cacheId, entry);
                    }
                  } catch (Exception e) {
                    errors.add(e);
                  }
                }));
    if (errors.size() > 0) {
      IllegalStateException e =
          new IllegalStateException("Couldn't parse some queries from the index");
      for (Exception parseError : errors) {
        e.addSuppressed(parseError);
      }
      throw e;
    }
  }

  public void scan(QueryCollector matcher) throws IOException {
    search(new MatchAllDocsQuery(), matcher);
  }

  long search(final Query query, QueryCollector matcher) throws IOException {
    QueryBuilder builder = termFilter -> query;
    return search(builder, matcher);
  }

  protected long searchInMemory(
      QueryBuilder queryBuilder,
      QueryCollector matcher,
      IndexSearcher searcher,
      Map<String, QueryCacheEntry> queries)
      throws IOException {
    MonitorQueryCollector collector = new MonitorQueryCollector(queries, matcher);
    long buildTime = System.nanoTime();
    Query query =
        queryBuilder.buildQuery(
            termFilters.get(searcher.getIndexReader().getReaderCacheHelper().getKey()));
    buildTime = System.nanoTime() - buildTime;
    searcher.search(query, collector);
    return buildTime;
  }

  abstract long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException;

  public abstract void purgeCache() throws IOException;

  abstract void purgeCache(CachePopulator populator) throws IOException;

  abstract int numDocs() throws IOException;

  public int cacheSize() {
    return queries.size();
  }

  abstract void deleteQueries(List<String> ids) throws IOException;

  abstract void clear() throws IOException;

  public long getLastPurged() {
    return lastPurged;
  }

  abstract void addListener(MonitorUpdateListener listener);

  public interface QueryCollector {

    void matchQuery(String id, QueryCacheEntry query, DataValues dataValues) throws IOException;

    default ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
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
    Scorable scorer;
    LeafReaderContext ctx;

    void advanceTo(int doc) throws IOException {
      assert scorer.docID() == doc;
      queryId.advanceExact(doc);
      cacheId.advanceExact(doc);
      if (mq != null) {
        mq.advanceExact(doc);
      }
    }
  }

  static class QueryTermFilter implements BiPredicate<String, BytesRef> {

    private final Map<String, BytesRefHash> termsHash = new HashMap<>();

    QueryTermFilter(IndexReader reader) throws IOException {
      for (LeafReaderContext ctx : reader.leaves()) {
        for (FieldInfo fi : ctx.reader().getFieldInfos()) {
          BytesRefHash terms = termsHash.computeIfAbsent(fi.name, f -> new BytesRefHash());
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

  // ---------------------------------------------
  //  Helper classes...
  // ---------------------------------------------

  /** A Collector that decodes the stored query for each document hit. */
  static final class MonitorQueryCollector extends SimpleCollector {

    private final Map<String, QueryCacheEntry> queries;
    private final QueryCollector matcher;
    private final DataValues dataValues = new DataValues();

    MonitorQueryCollector(Map<String, QueryCacheEntry> queries, QueryCollector matcher) {
      this.queries = queries;
      this.matcher = matcher;
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.dataValues.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      dataValues.advanceTo(doc);
      BytesRef cache_id = dataValues.cacheId.lookupOrd(dataValues.cacheId.ordValue());
      BytesRef query_id = dataValues.queryId.lookupOrd(dataValues.queryId.ordValue());
      QueryCacheEntry query = queries.get(cache_id.utf8ToString());
      matcher.matchQuery(query_id.utf8ToString(), query, dataValues);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.dataValues.cacheId = context.reader().getSortedDocValues(FIELDS.cache_id);
      this.dataValues.queryId = context.reader().getSortedDocValues(FIELDS.query_id);
      this.dataValues.mq = context.reader().getBinaryDocValues(FIELDS.mq);
      this.dataValues.ctx = context;
    }

    @Override
    public ScoreMode scoreMode() {
      return matcher.scoreMode();
    }
  }
}
