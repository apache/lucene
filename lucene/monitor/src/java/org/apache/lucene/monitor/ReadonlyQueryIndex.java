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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

class ReadonlyQueryIndex implements QueryIndex {
  private final SearcherManager manager;
  private final QueryDecomposer decomposer;
  private final MonitorQuerySerializer serializer;

  final Map<IndexReader.CacheKey, WritableQueryIndex.QueryTermFilter> termFilters = new HashMap<>();

  public ReadonlyQueryIndex(MonitorConfiguration config) throws IOException {
    if (config.getDirectoryProvider() == null) {
      throw new IllegalStateException(
          "You must specify a Directory when configuring a Monitor as read-only.");
    }
    Directory directory = config.getDirectoryProvider().get();
    this.manager = new SearcherManager(directory, new TermsHashBuilder(termFilters));
    this.decomposer = config.getQueryDecomposer();
    this.serializer = config.getQuerySerializer();
  }

  @Override
  public void commit(List<MonitorQuery> updates) throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot commit");
  }

  @Override
  public MonitorQuery getQuery(String queryId) throws IOException {
    if (serializer == null) {
      throw new IllegalStateException(
          "Cannot get queries from an index with no MonitorQuerySerializer");
    }
    BytesRef[] bytesHolder = new BytesRef[1];
    search(
        new TermQuery(new Term(WritableQueryIndex.FIELDS.query_id, queryId)),
        (id, query, dataValues) -> bytesHolder[0] = dataValues.mq.binaryValue());
    return serializer.deserialize(bytesHolder[0]);
  }

  @Override
  public void scan(QueryCollector matcher) throws IOException {
    search(new MatchAllDocsQuery(), matcher);
  }

  long search(final Query query, QueryCollector matcher) throws IOException {
    WritableQueryIndex.QueryBuilder builder = termFilter -> query;
    return search(builder, matcher);
  }

  static final class ReadonlyMonitorQueryCollector extends SimpleCollector {
    private final QueryCollector matcher;
    private final DataValues dataValues = new DataValues();
    private final MonitorQuerySerializer serializer;
    private final QueryDecomposer decomposer;

    ReadonlyMonitorQueryCollector(
        QueryCollector matcher, MonitorQuerySerializer serializer, QueryDecomposer decomposer) {
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
              .orElse(null);
      matcher.matchQuery(query_id.utf8ToString(), query, dataValues);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.dataValues.cacheId =
          context.reader().getSortedDocValues(WritableQueryIndex.FIELDS.cache_id);
      this.dataValues.queryId =
          context.reader().getSortedDocValues(WritableQueryIndex.FIELDS.query_id);
      this.dataValues.mq = context.reader().getBinaryDocValues(WritableQueryIndex.FIELDS.mq);
      this.dataValues.ctx = context;
    }

    @Override
    public ScoreMode scoreMode() {
      return matcher.scoreMode();
    }
  }

  @Override
  public long search(QueryIndex.QueryBuilder queryBuilder, QueryCollector matcher)
      throws IOException {
    IndexSearcher searcher = null;
    try {
      searcher = manager.acquire();

      ReadonlyMonitorQueryCollector collector =
          new ReadonlyMonitorQueryCollector(matcher, serializer, decomposer);
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
    throw new IllegalStateException("Monitor is readOnly cannot purge cache");
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(manager);
  }

  @Override
  public int numDocs() throws IOException {
    IndexSearcher searcher = manager.acquire();
    return searcher.getIndexReader().numDocs();
  }

  @Override
  public int cacheSize() {
    return 0;
  }

  @Override
  public void deleteQueries(Iterable<String> ids) throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot delete queries");
  }

  @Override
  public void clear() throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot clear");
  }
}
