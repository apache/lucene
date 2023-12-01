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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Objects;
import org.apache.lucene.search.Query;

/**
 * This class is used to hold the results of a query until the results of each segment has been
 * read. Queries sent to PIM return results for all segments of the index, then results are cached
 * and provided to Lucene's search APIs on a per-segment basis.
 * The key to the cache is the query combined with the thread id. This means we prevent a search thread to
 * read the results of the same query issued by another search thread.
 */
public class PimMatchCache {

  private static final int defaultCacheSize = 1024;
  private final int cacheSize;
  private ConcurrentHashMap<CacheElem, DpuResultsReader> cache;

  public PimMatchCache() {
    this(defaultCacheSize);
  }

  public PimMatchCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache = new ConcurrentHashMap<>();
  }

  DpuResultsReader get(Query query, boolean remove) throws IOException {

    CacheElem e = new CacheElem(query, Thread.currentThread().getId());
    if (remove) return cache.remove(e);
    else return cache.get(e);
  }

  boolean put(Query query, DpuResultsReader results) {
    assert results != null;
    if (cache.size() < cacheSize) {
      cache.put(new CacheElem(query, Thread.currentThread().getId()), results);
      return true;
    }
    return false;
  }

  /**
   * Class used as the key in the query results cache.
   * This combines the query object and the thread id.
   */
  private class CacheElem {

    public Query query;
    public long tid;

    CacheElem(Query query, long tid) {
      this.query = query;
      this.tid = tid;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof CacheElem))
        return false;
      CacheElem other = (CacheElem) o;
      boolean queryEquals = (this.query == null && other.query == null)
              || (this.query != null && this.query.equals(other.query));
      return this.tid == other.tid && queryEquals;
    }

    @Override
    public final int hashCode() {
      return Objects.hash(query, tid);
    }
  }
}
