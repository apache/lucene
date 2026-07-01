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

import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestKnnSearchStrategy extends LuceneTestCase {

  public void testHnswDefaultConstructorHasNoQueryBitHint() {
    KnnSearchStrategy.Hnsw strategy = new KnnSearchStrategy.Hnsw(60);
    assertNull(strategy.queryBitSizeHint());
  }

  public void testHnswConstructorWithQueryBitHint() {
    KnnSearchStrategy.Hnsw strategy = new KnnSearchStrategy.Hnsw(60, 4);
    assertEquals(Integer.valueOf(4), strategy.queryBitSizeHint());
  }

  public void testHnswQueryBitHintValidation() {
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> new KnnSearchStrategy.Hnsw(60, 0));
    assertEquals("queryBitSizeHint must be > 0", exception.getMessage());
  }

  public void testHnswEqualsAndHashCodeIncludeQueryBitHint() {
    KnnSearchStrategy.Hnsw withHintA = new KnnSearchStrategy.Hnsw(60, 4);
    KnnSearchStrategy.Hnsw withHintB = new KnnSearchStrategy.Hnsw(60, 4);
    KnnSearchStrategy.Hnsw withDifferentHint = new KnnSearchStrategy.Hnsw(60, 2);
    KnnSearchStrategy.Hnsw withoutHint = new KnnSearchStrategy.Hnsw(60);

    assertEquals(withHintA, withHintB);
    assertEquals(withHintA.hashCode(), withHintB.hashCode());
    assertNotEquals(withHintA, withDifferentHint);
    assertNotEquals(withHintA, withoutHint);
  }

  public void testSeededPreservesOriginalHnswQueryBitHint() {
    KnnSearchStrategy.Hnsw original = new KnnSearchStrategy.Hnsw(60, 2);
    KnnSearchStrategy.Seeded seeded =
        new KnnSearchStrategy.Seeded(DocIdSetIterator.empty(), 0, original);

    assertSame(original, seeded.originalStrategy());
    assertEquals(
        Integer.valueOf(2),
        ((KnnSearchStrategy.Hnsw) seeded.originalStrategy()).queryBitSizeHint());
  }

  public void testPatienceWrappingPreservesQueryBitHint() {
    KnnSearchStrategy.Hnsw strategy = new KnnSearchStrategy.Hnsw(60, 1);
    TopKnnCollector collector = new TopKnnCollector(2, 10, strategy);
    HnswQueueSaturationCollector wrapped = new HnswQueueSaturationCollector(collector, 0.99d, 5);

    KnnSearchStrategy wrappedStrategy = wrapped.getSearchStrategy();
    assertTrue(wrappedStrategy instanceof KnnSearchStrategy.Patience);
    assertEquals(
        Integer.valueOf(1), ((KnnSearchStrategy.Patience) wrappedStrategy).queryBitSizeHint());
  }

  public void testPatienceEqualsAndHashCodeIncludeQueryBitHint() {
    TopKnnCollector collector = new TopKnnCollector(2, 10, KnnSearchStrategy.Hnsw.DEFAULT);
    HnswQueueSaturationCollector wrapped = new HnswQueueSaturationCollector(collector, 0.99d, 5);
    KnnSearchStrategy.Patience withHintA = new KnnSearchStrategy.Patience(wrapped, 60, 4);
    KnnSearchStrategy.Patience withHintB = new KnnSearchStrategy.Patience(wrapped, 60, 4);
    KnnSearchStrategy.Patience withDifferentHint = new KnnSearchStrategy.Patience(wrapped, 60, 2);

    assertEquals(withHintA, withHintB);
    assertEquals(withHintA.hashCode(), withHintB.hashCode());
    assertNotEquals(withHintA, withDifferentHint);
  }
}
