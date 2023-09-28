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
import org.apache.lucene.search.Query;

/**
 * This class is used to hold the results of a query until the results of each segment has been
 * read. Queries sent to PIM return results for all segments of the index, then results are cached
 * and provided to Lucene's search APIs on a per-segment basis.
 */
public class PimMatchCache {

  private static final int defaultCacheSize = 1024;
  private final int cacheSize;

  public PimMatchCache() {
    this(defaultCacheSize);
  }

  public PimMatchCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache = new ConcurrentHashMap<>();
  }

  DpuResultsReader get(Query query, boolean remove) throws IOException {

    if (remove) return cache.remove(query);
    else return cache.get(query);
  }

  boolean put(Query query, DpuResultsReader results) {
    assert results != null;
    if (cache.size() < cacheSize) {
      cache.put(query, results);
      return true;
    }
    return false;
  }

  private ConcurrentHashMap<Query, DpuResultsReader> cache;
}
