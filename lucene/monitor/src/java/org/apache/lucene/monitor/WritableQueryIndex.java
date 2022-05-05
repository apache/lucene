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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;

class WritableQueryIndex extends QueryIndex {

  private final IndexWriter writer;
  private final Presearcher presearcher;

  /* Used to cache updates while a purge is ongoing */
  private volatile Map<String, QueryCacheEntry> purgeCache = null;

  /* Used to lock around the creation of the purgeCache */
  private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();
  private final Object commitLock = new Object();

  private final ScheduledExecutorService purgeExecutor;

  /* The current query cache */
  // NB this is not final because it can be replaced by purgeCache()
  protected volatile ConcurrentMap<String, QueryCacheEntry> queries;

  protected long lastPurged = -1;

  WritableQueryIndex(MonitorConfiguration configuration, Presearcher presearcher)
      throws IOException {

    this.writer = configuration.buildIndexWriter();
    this.queries = new ConcurrentHashMap<>();
    this.manager = new SearcherManager(writer, true, true, new TermsHashBuilder(termFilters));
    this.decomposer = configuration.getQueryDecomposer();
    this.serializer = configuration.getQuerySerializer();
    this.presearcher = presearcher;
    populateQueryCache(serializer, decomposer);

    long purgeFrequency = configuration.getPurgeFrequency();
    this.purgeExecutor =
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("cache-purge"));
    this.purgeExecutor.scheduleAtFixedRate(
        () -> {
          try {
            purgeCache();
          } catch (Throwable e) {
            listeners.forEach(l -> l.onPurgeError(e));
          }
        },
        purgeFrequency,
        purgeFrequency,
        configuration.getPurgeFrequencyUnits());
  }

  @Override
  public void commit(List<MonitorQuery> updates) throws IOException {
    commitWithoutNotify(updates);
    listeners.forEach(l -> l.afterUpdate(updates));
  }

  private void commitWithoutNotify(List<MonitorQuery> updates) throws IOException {
    List<Indexable> indexables = buildIndexables(updates);
    synchronized (commitLock) {
      purgeLock.readLock().lock();
      try {
        if (indexables.size() > 0) {
          Set<String> ids = new HashSet<>();
          for (Indexable update : indexables) {
            ids.add(update.queryCacheEntry.queryId);
          }
          for (String id : ids) {
            writer.deleteDocuments(new Term(FIELDS.query_id, id));
          }
          for (Indexable update : indexables) {
            this.queries.put(update.queryCacheEntry.cacheId, update.queryCacheEntry);
            writer.addDocument(update.document);
            if (purgeCache != null)
              purgeCache.put(update.queryCacheEntry.cacheId, update.queryCacheEntry);
          }
        }
        writer.commit();
        manager.maybeRefresh();
      } finally {
        purgeLock.readLock().unlock();
      }
    }
  }

  private static class Indexable {
    final QueryCacheEntry queryCacheEntry;
    final Document document;

    private Indexable(QueryCacheEntry queryCacheEntry, Document document) {
      this.queryCacheEntry = queryCacheEntry;
      this.document = document;
    }
  }

  private void populateQueryCache(MonitorQuerySerializer serializer, QueryDecomposer decomposer)
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

  private static final BytesRef EMPTY = new BytesRef();

  private List<Indexable> buildIndexables(List<MonitorQuery> updates) {
    List<Indexable> indexables = new ArrayList<>();
    for (MonitorQuery mq : updates) {
      if (serializer != null && mq.getQueryString() == null) {
        throw new IllegalArgumentException(
            "Cannot add a MonitorQuery with a null string representation to a non-ephemeral Monitor");
      }
      BytesRef serialized = serializer == null ? EMPTY : serializer.serialize(mq);
      for (QueryCacheEntry qce : QueryCacheEntry.decompose(mq, decomposer)) {
        Document doc = presearcher.indexQuery(qce.matchQuery, mq.getMetadata());
        doc.add(new StringField(FIELDS.query_id, qce.queryId, Field.Store.NO));
        doc.add(new SortedDocValuesField(FIELDS.cache_id, new BytesRef(qce.cacheId)));
        doc.add(new SortedDocValuesField(FIELDS.query_id, new BytesRef(qce.queryId)));
        doc.add(new BinaryDocValuesField(FIELDS.mq, serialized));
        indexables.add(new Indexable(qce, doc));
      }
    }
    return indexables;
  }

  @Override
  public long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException {
    IndexSearcher searcher = null;
    try {
      Map<String, QueryCacheEntry> queries;

      purgeLock.readLock().lock();
      try {
        searcher = manager.acquire();
        queries = this.queries;
      } finally {
        purgeLock.readLock().unlock();
      }

      MonitorQueryCollector collector = new MonitorQueryCollector(queries, matcher);
      long buildTime = System.nanoTime();
      Query query =
          queryBuilder.buildQuery(
              termFilters.get(searcher.getIndexReader().getReaderCacheHelper().getKey()));
      buildTime = System.nanoTime() - buildTime;
      searcher.search(query, collector);
      return buildTime;
    } finally {
      if (searcher != null) {
        manager.release(searcher);
      }
    }
  }

  @Override
  public void purgeCache() throws IOException {
    purgeCache(
        newCache ->
            scan(
                (id, query, dataValues) -> {
                  if (query != null) newCache.put(query.cacheId, query);
                }));
    lastPurged = System.nanoTime();
    listeners.forEach(MonitorUpdateListener::onPurge);
  }

  @Override
  /**
   * Remove unused queries from the query cache.
   *
   * <p>This is normally called from a background thread at a rate set by configurePurgeFrequency().
   *
   * @throws IOException on IO errors
   */
  synchronized void purgeCache(CachePopulator populator) throws IOException {

    // Note on implementation

    // The purge works by scanning the query index and creating a new query cache populated
    // for each query in the index.  When the scan is complete, the old query cache is swapped
    // for the new, allowing it to be garbage-collected.

    // In order to not drop cached queries that have been added while a purge is ongoing,
    // we use a ReadWriteLock to guard the creation and removal of an register log.  Commits take
    // the read lock.  If the register log has been created, then a purge is ongoing, and queries
    // are added to the register log within the read lock guard.

    // The purge takes the write lock when creating the register log, and then when swapping out
    // the old query cache.  Within the second write lock guard, the contents of the register log
    // are added to the new query cache, and the register log itself is removed.

    final ConcurrentMap<String, QueryCacheEntry> newCache = new ConcurrentHashMap<>();

    purgeLock.writeLock().lock();
    try {
      purgeCache = new ConcurrentHashMap<>();
    } finally {
      purgeLock.writeLock().unlock();
    }

    populator.populateCacheWithIndex(newCache);

    purgeLock.writeLock().lock();
    try {
      newCache.putAll(purgeCache);
      purgeCache = null;
      queries = newCache;
    } finally {
      purgeLock.writeLock().unlock();
    }
  }

  // ---------------------------------------------
  //  Proxy trivial operations...
  // ---------------------------------------------

  @Override
  public void close() throws IOException {
    purgeExecutor.shutdown();
    IOUtils.close(manager, writer, writer.getDirectory());
  }

  @Override
  public int numDocs() throws IOException {
    return writer.getDocStats().numDocs;
  }

  @Override
  public int cacheSize() {
    return queries.size();
  }

  @Override
  public void deleteQueries(List<String> ids) throws IOException {
    for (String id : ids) {
      writer.deleteDocuments(new Term(FIELDS.query_id, id));
    }
    commitWithoutNotify(Collections.emptyList());
    listeners.forEach(l -> l.afterDelete(ids));
  }

  @Override
  public void clear() throws IOException {
    writer.deleteAll();
    commitWithoutNotify(Collections.emptyList());
    listeners.forEach(MonitorUpdateListener::afterClear);
  }

  @Override
  public long getLastPurged() {
    return lastPurged;
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
