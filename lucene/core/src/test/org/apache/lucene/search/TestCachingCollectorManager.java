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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestCachingCollectorManager extends LuceneTestCase {

  public void testCacheOverflow() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < atLeast(10); i++) {
      iw.addDocument(new Document());
    }
    IndexSearcher searcher = newSearcher(iw.getReader());
    iw.close();

    CachingCollectorManager<TopScoreDocCollector, TopDocs> caching =
        new CachingCollectorManager<>(
            new TopScoreDocCollectorManager(10, Integer.MAX_VALUE), false, null, 0);

    searcher.search(MatchAllDocsQuery.INSTANCE, caching);
    assertFalse(caching.isCached());
    assertThrows(
        IllegalStateException.class,
        () -> caching.replay(new TopScoreDocCollectorManager(10, Integer.MAX_VALUE)));

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testNotCachedBeforeSearch() {
    CachingCollectorManager<TopScoreDocCollector, TopDocs> caching =
        new CachingCollectorManager<>(
            new TopScoreDocCollectorManager(10, Integer.MAX_VALUE), false, null, Integer.MAX_VALUE);
    assertFalse(caching.isCached());

    assertThrows(
        IllegalStateException.class,
        () -> caching.replay(new TopScoreDocCollectorManager(10, Integer.MAX_VALUE)));
  }

  public void testBasic() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 10; i++) {
      iw.addDocument(new Document());
    }
    IndexSearcher searcher = newSearcher(iw.getReader());
    iw.close();

    CachingCollectorManager<TopScoreDocCollector, TopDocs> caching =
        new CachingCollectorManager<>(
            new TopScoreDocCollectorManager(10, Integer.MAX_VALUE), true, null, Integer.MAX_VALUE);

    TopDocs firstResult = searcher.search(MatchAllDocsQuery.INSTANCE, caching);
    assertTrue(caching.isCached());
    assertEquals(10, firstResult.totalHits.value());

    TopDocs replayResult = caching.replay(new TopScoreDocCollectorManager(10, Integer.MAX_VALUE));
    assertEquals(firstResult.totalHits.value(), replayResult.totalHits.value());
    assertEquals(firstResult.scoreDocs.length, replayResult.scoreDocs.length);

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testConstructor() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CachingCollectorManager<>(
                new TopScoreDocCollectorManager(10, Integer.MAX_VALUE), false, null, null));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CachingCollectorManager<>(
                new TopScoreDocCollectorManager(10, Integer.MAX_VALUE), false, 1.0, 1));
  }
}
