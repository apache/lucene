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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;

class ReadonlyQueryIndex extends QueryIndex {

  private final ScheduledExecutorService refreshExecutor;

  public ReadonlyQueryIndex(MonitorConfiguration configuration) throws IOException {
    if (configuration.getDirectoryProvider() == null) {
      throw new IllegalStateException(
          "You must specify a Directory when configuring a Monitor as read-only.");
    }
    Directory directory = configuration.getDirectoryProvider().get();
    this.manager = new SearcherManager(directory, new TermsHashBuilder(termFilters));
    this.decomposer = configuration.getQueryDecomposer();
    this.serializer = configuration.getQuerySerializer();
    this.refreshExecutor =
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("cache-purge"));
    long refreshFrequency = configuration.getPurgeFrequency();
    this.refreshExecutor.scheduleAtFixedRate(
        () -> {
          try {
            this.purgeCache();
          } catch (IOException e) {
            listeners.forEach(l -> l.onPurgeError(e));
          }
        },
        refreshFrequency,
        refreshFrequency,
        configuration.getPurgeFrequencyUnits());
  }

  @Override
  public void commit(List<MonitorQuery> updates) throws IOException {
    throw new UnsupportedOperationException("Monitor is readOnly cannot commit");
  }

  @Override
  public long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException {
    IndexSearcher searcher = null;
    try {
      searcher = manager.acquire();
      LazyMonitorQueryCollector collector =
          new LazyMonitorQueryCollector(matcher, serializer, decomposer);
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
    manager.maybeRefresh();
    listeners.forEach(MonitorUpdateListener::onPurge);
  }

  @Override
  void purgeCache(CachePopulator populator) {
    throw new UnsupportedOperationException("Monitor is readOnly, it has no cache");
  }

  @Override
  public void close() throws IOException {
    refreshExecutor.shutdown();
    IOUtils.close(manager);
  }

  @Override
  public int numDocs() throws IOException {
    IndexSearcher searcher = null;
    int numDocs;
    try {
      searcher = manager.acquire();
      numDocs = searcher.getIndexReader().numDocs();
    } finally {
      if (searcher != null) {
        manager.release(searcher);
      }
    }
    return numDocs;
  }

  @Override
  public int cacheSize() {
    return -1;
  }

  @Override
  public void deleteQueries(List<String> ids) throws IOException {
    throw new UnsupportedOperationException("Monitor is readOnly cannot delete queries");
  }

  @Override
  public void clear() throws IOException {
    throw new UnsupportedOperationException("Monitor is readOnly cannot clear");
  }

  @Override
  public long getLastPurged() {
    return -1;
  }

  // ---------------------------------------------
  //  Helper classes...
  // ---------------------------------------------

  /** A Collector that decodes the stored query for each document hit reparsing them everytime. */
  static final class LazyMonitorQueryCollector extends SimpleCollector {
    private final QueryIndex.QueryCollector matcher;
    private final QueryIndex.DataValues dataValues = new QueryIndex.DataValues();
    private final MonitorQuerySerializer serializer;
    private final QueryDecomposer decomposer;

    LazyMonitorQueryCollector(
        QueryIndex.QueryCollector matcher,
        MonitorQuerySerializer serializer,
        QueryDecomposer decomposer) {
      this.matcher = matcher;
      this.serializer = serializer;
      this.decomposer = decomposer;
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
      MonitorQuery mq = serializer.deserialize(dataValues.mq.binaryValue());
      QueryCacheEntry query =
          QueryCacheEntry.decompose(mq, decomposer).stream()
              .filter(queryCacheEntry -> queryCacheEntry.cacheId.equals(cache_id.utf8ToString()))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("Cached queries not found"));
      matcher.matchQuery(query_id.utf8ToString(), query, dataValues);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.dataValues.cacheId = context.reader().getSortedDocValues(QueryIndex.FIELDS.cache_id);
      this.dataValues.queryId = context.reader().getSortedDocValues(QueryIndex.FIELDS.query_id);
      this.dataValues.mq = context.reader().getBinaryDocValues(QueryIndex.FIELDS.mq);
      this.dataValues.ctx = context;
    }

    @Override
    public ScoreMode scoreMode() {
      return matcher.scoreMode();
    }
  }
}
