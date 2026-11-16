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
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;

class TermsHashBuilder extends SearcherFactory {
  private final Map<IndexReader.CacheKey, QueryIndex.QueryTermFilter> termFilters;
  private final Executor executor;
  private final int maxQueriesPerSlice;

  TermsHashBuilder(
      Map<IndexReader.CacheKey, QueryIndex.QueryTermFilter> termFilters,
      MonitorConfiguration configuration) {
    this.termFilters = termFilters;
    this.executor = configuration.getExecutor();
    this.maxQueriesPerSlice = configuration.getMaxQueriesPerSlice();
  }

  @Override
  public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
      throws IOException {
    IndexSearcher searcher = new QueryIndexSearcher(reader, executor, maxQueriesPerSlice);
    searcher.setQueryCache(null);
    termFilters.put(reader.getReaderCacheHelper().getKey(), new QueryIndex.QueryTermFilter(reader));
    reader.getReaderCacheHelper().addClosedListener(termFilters::remove);
    return searcher;
  }

  /**
   * Slices the queryindex by query count, grouping small segments and splitting large ones, because
   * matching a query costs far more than an ordinary collect.
   */
  static final class QueryIndexSearcher extends IndexSearcher {

    private final int maxQueriesPerSlice;

    QueryIndexSearcher(IndexReader reader, Executor executor, int maxQueriesPerSlice) {
      super(reader, executor);
      this.maxQueriesPerSlice = maxQueriesPerSlice;
    }

    @Override
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
      return slices(leaves, maxQueriesPerSlice, Integer.MAX_VALUE, true);
    }
  }
}
