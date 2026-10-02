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
package org.apache.lucene.tests.util;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.util.IOUtils;

/**
 * Replaces {@link IndexSearcher}'s default query cache and caching policy with a fresh cache and a
 * policy that randomly caches. The previous defaults are restored afterwards, so this callback can
 * be nested (suite level and test level).
 */
final class SetupAndRestoreQueryCache implements BeforeAfterCallback {
  private QueryCache previousCache;
  private QueryCachingPolicy previousPolicy;
  private LRUQueryCache cache;

  @Override
  public void before() {
    previousCache = IndexSearcher.getDefaultQueryCache();
    previousPolicy = IndexSearcher.getDefaultQueryCachingPolicy();

    cache = new LRUQueryCache(10000, 1 << 25, _ -> true, Float.POSITIVE_INFINITY);
    IndexSearcher.setDefaultQueryCache(cache);
    IndexSearcher.setDefaultQueryCachingPolicy(LuceneTestCaseParent.MAYBE_CACHE_POLICY);
  }

  @Override
  public void after() throws IOException {
    IndexSearcher.setDefaultQueryCache(previousCache);
    IndexSearcher.setDefaultQueryCachingPolicy(previousPolicy);
    if (cache != null) {
      cache.clear();
    }
    IOUtils.close(cache);
    cache = null;
  }
}
