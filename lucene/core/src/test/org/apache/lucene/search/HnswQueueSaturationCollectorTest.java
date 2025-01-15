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
public class HnswQueueSaturationCollectorTest extends LuceneTestCase {

  @Test
  public void testDelegate() {
    Random random = random();
    int numDocs = 100;
    int k = random.nextInt(10);
    KnnCollector delegate = new TopKnnCollector(k, numDocs);
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate);
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
  public void testEarlyExit() {
    Random random = random();
    int numDocs = 10000;
    int k = random.nextInt(100);
    KnnCollector delegate = new TopKnnCollector(k, numDocs);
    HnswQueueSaturationCollector queueSaturationCollector =
        new HnswQueueSaturationCollector(delegate);
    for (int i = 0; i < random.nextInt(numDocs); i++) {
      queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
      boolean earlyTerminatedSaturation = queueSaturationCollector.earlyTerminated();
      boolean earlyTerminatedDelegate = delegate.earlyTerminated();
      assertTrue(earlyTerminatedSaturation || !earlyTerminatedDelegate);
      if (earlyTerminatedDelegate) {
        assertTrue(earlyTerminatedSaturation);
      }
      if (!earlyTerminatedSaturation) {
        assertFalse(earlyTerminatedSaturation);
      }
    }
  }
}
