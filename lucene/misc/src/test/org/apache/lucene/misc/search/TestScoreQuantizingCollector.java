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
package org.apache.lucene.misc.search;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestScoreQuantizingCollector extends LuceneTestCase {

  public void testValidation() {
    Collector collector = TopScoreDocCollector.create(10, 10);
    expectThrows(IllegalArgumentException.class, () -> new ScoreQuantizingCollector(collector, 0));
    expectThrows(IllegalArgumentException.class, () -> new ScoreQuantizingCollector(collector, 24));
    for (int i = 1; i <= 23; ++i) {
      new ScoreQuantizingCollector(collector, i); // no exception
    }
  }

  public void testQuantize() {
    ScoreQuantizingCollector collector =
        new ScoreQuantizingCollector(TopScoreDocCollector.create(10, 10), 4);

    assertEquals(1.125f, collector.roundDown(1.2345f), 0f);
    assertEquals(1.25f, collector.roundUp(1.2345f), 0f);

    assertEquals(1.25f, collector.roundDown(1.25f), 0f);
    assertEquals(1.25f, collector.roundUp(1.25f), 0f);

    assertEquals(0f, collector.roundDown(0f), 0f);
    assertEquals(0f, collector.roundUp(0f), 0f);

    assertEquals(0f, collector.roundDown(Float.MIN_VALUE), 0f);
    assertEquals(0f, collector.roundUp(0f), 0f);

    assertEquals(Float.MIN_NORMAL, collector.roundDown(Float.MIN_NORMAL), 0f);
    assertEquals(Float.MIN_NORMAL, collector.roundUp(Float.MIN_NORMAL), 0f);

    assertEquals(3.1901472E38f, collector.roundDown(Float.MAX_VALUE), 0f);
    assertEquals(Float.POSITIVE_INFINITY, collector.roundUp(Float.MAX_VALUE), 0f);
  }
}
