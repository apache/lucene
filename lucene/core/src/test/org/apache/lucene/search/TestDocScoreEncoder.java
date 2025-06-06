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

public class TestDocScoreEncoder extends LuceneTestCase {

  public void testRandom() {
    for (int i = 0; i < 1000; i++) {
      doAssert(
          Float.intBitsToFloat(random().nextInt()),
          random().nextInt(Integer.MAX_VALUE),
          Float.intBitsToFloat(random().nextInt()),
          random().nextInt(Integer.MAX_VALUE));
    }
  }

  public void testSameDoc() {
    for (int i = 0; i < 1000; i++) {
      doAssert(
          Float.intBitsToFloat(random().nextInt()), 1, Float.intBitsToFloat(random().nextInt()), 1);
    }
  }

  public void testSameScore() {
    for (int i = 0; i < 1000; i++) {
      doAssert(1f, random().nextInt(Integer.MAX_VALUE), 1f, random().nextInt(Integer.MAX_VALUE));
    }
  }

  private void doAssert(float score1, int doc1, float score2, int doc2) {
    if (Float.isNaN(score1) || Float.isNaN(score2)) {
      return;
    }

    long code1 = DocScoreEncoder.encode(doc1, score1);
    long code2 = DocScoreEncoder.encode(doc2, score2);

    assertEquals(doc1, DocScoreEncoder.docId(code1));
    assertEquals(doc2, DocScoreEncoder.docId(code2));
    assertEquals(score1, DocScoreEncoder.toScore(code1), 0f);
    assertEquals(score2, DocScoreEncoder.toScore(code2), 0f);

    if (score1 < score2) {
      assertTrue(code1 < code2);
    } else if (score1 > score2) {
      assertTrue(code1 > code2);
    } else if (doc1 == doc2) {
      assertEquals(code1, code2);
    } else {
      assertEquals(code1 > code2, doc1 < doc2);
    }
  }
}
