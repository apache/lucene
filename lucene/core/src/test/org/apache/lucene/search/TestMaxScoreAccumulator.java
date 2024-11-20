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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestMaxScoreAccumulator extends LuceneTestCase {
  public void testSimple() {
    MaxScoreAccumulator acc = new MaxScoreAccumulator();
    acc.accumulate(0, 0f);
    assertEquals(0f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(0, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(10, 0f);
    assertEquals(0f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(0, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(100, 1000f);
    assertEquals(1000f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(100, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(1000, 5f);
    assertEquals(1000f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(100, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(99, 1000f);
    assertEquals(1000f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(99, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(1000, 1001f);
    assertEquals(1001f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(1000, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(10, 1001f);
    assertEquals(1001f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(10, MaxScoreAccumulator.docId(acc.getRaw()), 0);
    acc.accumulate(100, 1001f);
    assertEquals(1001f, MaxScoreAccumulator.toScore(acc.getRaw()), 0);
    assertEquals(10, MaxScoreAccumulator.docId(acc.getRaw()), 0);
  }
}
