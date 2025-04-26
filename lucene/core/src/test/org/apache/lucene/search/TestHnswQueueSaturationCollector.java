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

import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

/** Tests for {@link HnswQueueSaturationCollector} */
public class TestHnswQueueSaturationCollector extends LuceneTestCase {

  @Test
  public void testDelegate() {
    Random random = random();
    int numDocs = 100;
    int k = random.nextInt(1, 10);
    KnnCollector delegate = new TopKnnCollector(k, numDocs);
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate, 1, Integer.MAX_VALUE);
    for (int i = 0; i < random.nextInt(numDocs); i++) {
      queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
    }
    assertEquals(delegate.k(), queueSaturationCollector.k());
    assertEquals(delegate.visitedCount(), queueSaturationCollector.visitedCount());
    assertEquals(delegate.visitLimit(), queueSaturationCollector.visitLimit());
    assertEquals(
        delegate.minCompetitiveSimilarity(),
        queueSaturationCollector.minCompetitiveSimilarity(),
        1e-3);
  }

  @Test
  public void testEarlyExpectedExit() {
    int numDocs = 1000;
    int k = 10;
    KnnCollector delegate = new TopKnnCollector(k, numDocs);
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate, 0.9, 10);
    for (int i = 0; i < numDocs; i++) {
      queueSaturationCollector.collect(i, 1.0f - i * 1e-3f);
      if (i % 10 == 0) {
        queueSaturationCollector.nextCandidate();
      }
      if (queueSaturationCollector.earlyTerminated()) {
        assertEquals(120, i);
        break;
      }
    }
  }

  @Test
  public void testDelegateVsSaturateEarlyExit() {
    Random random = random();
    int numDocs = 10000;
    int k = random.nextInt(1, 100);
    KnnCollector delegate = new TopKnnCollector(k, numDocs);
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate, 0.5, 1);
    for (int i = 0; i < random.nextInt(numDocs); i++) {
      queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
      if (i % 10 == 0) {
        queueSaturationCollector.nextCandidate();
      }
      boolean earlyTerminatedSaturation = queueSaturationCollector.earlyTerminated();
      boolean earlyTerminatedDelegate = delegate.earlyTerminated();
      assertTrue(earlyTerminatedSaturation || !earlyTerminatedDelegate);
    }
  }

  @Test
  public void testEarlyExitRelation() {
    Random random = random();
    int numDocs = 10000;
    int k = random.nextInt(1, 100);
    KnnCollector delegate = new TopKnnCollector(k, random.nextInt(numDocs));
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate, 0.5, 1);
    for (int i = 0; i < random.nextInt(numDocs); i++) {
      queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
      if (i % 10 == 0) {
        queueSaturationCollector.nextCandidate();
      }
      if (delegate.earlyTerminated()) {
        TopDocs topDocs = queueSaturationCollector.topDocs();
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation());
      }
      if (queueSaturationCollector.earlyTerminated()) {
        TopDocs topDocs = queueSaturationCollector.topDocs();
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation());
        break;
      }
    }
  }
}
