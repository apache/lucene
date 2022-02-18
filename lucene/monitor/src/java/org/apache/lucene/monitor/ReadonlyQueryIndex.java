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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

class ReadonlyQueryIndex extends QueryIndex {

  public ReadonlyQueryIndex(MonitorConfiguration configuration) throws IOException {
    if (configuration.getDirectoryProvider() == null) {
      throw new IllegalStateException(
          "You must specify a Directory when configuring a Monitor as read-only.");
    }
    Directory directory = configuration.getDirectoryProvider().get();
    this.manager = new SearcherManager(directory, new TermsHashBuilder(termFilters));
    this.decomposer = configuration.getQueryDecomposer();
    this.serializer = configuration.getQuerySerializer();
    this.populateQueryCache(serializer, decomposer);
  }

  @Override
  public void commit(List<MonitorQuery> updates) throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot commit");
  }

  @Override
  long search(final Query query, QueryCollector matcher) throws IOException {
    QueryBuilder builder = termFilter -> query;
    return search(builder, matcher);
  }

  @Override
  public long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException {
    IndexSearcher searcher = null;
    try {
      searcher = manager.acquire();
      return searchInMemory(queryBuilder, matcher, searcher, this.queries);
    } finally {
      if (searcher != null) {
        manager.release(searcher);
      }
    }
  }

  @Override
  void purgeCache(CachePopulator populator) throws IOException {
    final ConcurrentMap<String, QueryCacheEntry> newCache = new ConcurrentHashMap<>();
    populator.populateCacheWithIndex(newCache);
    queries = newCache;
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
  public void deleteQueries(List<String> ids) throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot delete queries");
  }

  @Override
  public void clear() throws IOException {
    throw new IllegalStateException("Monitor is readOnly cannot clear");
  }

  @Override
  public void addListener(MonitorUpdateListener listener) {
    throw new IllegalStateException("Monitor is readOnly cannot register listeners");
  }
}
