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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestDocIdSetIterator extends LuceneTestCase {
  public void testRangeBasic() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 8);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(6, disi.nextDoc());
    assertEquals(7, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testInvalidRange() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(5, 4);
        });
  }

  public void testInvalidMin() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(-1, 4);
        });
  }

  public void testEmpty() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(7, 7);
        });
  }

  public void testAdvance() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 20);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(17, disi.advance(17));
    assertEquals(18, disi.nextDoc());
    assertEquals(19, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testIntoBitset() throws Exception {
    for (int i = 0; i < 10; i++) {
      int max = 1 + random().nextInt(500);
      DocIdSetIterator expectedDisi;
      DocIdSetIterator actualDisi;
      if ((i & 1) == 0) {
        int min = random().nextInt(max);
        expectedDisi = DocIdSetIterator.range(min, max);
        actualDisi = DocIdSetIterator.range(min, max);
      } else {
        expectedDisi = DocIdSetIterator.all(max);
        actualDisi = DocIdSetIterator.all(max);
      }
      FixedBitSet expected = new FixedBitSet(max * 2);
      FixedBitSet actual = new FixedBitSet(max * 2);
      int doc = -1;
      expectedDisi.nextDoc();
      actualDisi.nextDoc();
      while (doc != NO_MORE_DOCS) {
        int r = random().nextInt(3);
        switch (r) {
          case 0 -> {
            expectedDisi.nextDoc();
            actualDisi.nextDoc();
          }
          case 1 -> {
            int jump = expectedDisi.docID() + random().nextInt(5);
            expectedDisi.advance(jump);
            actualDisi.advance(jump);
          }
          case 2 -> {
            expected.clear();
            actual.clear();
            int upTo =
                random().nextBoolean()
                    ? expectedDisi.docID() - 1
                    : expectedDisi.docID() + random().nextInt(5);
            int offset = expectedDisi.docID() - random().nextInt(max);
            // use the default impl of intoBitSet
            new FilterDocIdSetIterator(expectedDisi).intoBitSet(upTo, expected, offset);
            actualDisi.intoBitSet(upTo, actual, offset);
            assertArrayEquals(expected.getBits(), actual.getBits());
          }
        }
        assertEquals(expectedDisi.docID(), actualDisi.docID());
        doc = expectedDisi.docID();
      }
    }
  }

  public void testDocIDRunEnd() throws IOException {
    DocIdSetIterator it = DocIdSetIterator.all(13);
    assertEquals(0, it.nextDoc());
    assertEquals(13, it.docIDRunEnd());
    assertEquals(10, it.advance(10));
    assertEquals(13, it.docIDRunEnd());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(13));
  }

  public void testDocIDRunEndRange() throws IOException {
    DocIdSetIterator it = DocIdSetIterator.range(4, 13);
    assertEquals(4, it.nextDoc());
    assertEquals(13, it.docIDRunEnd());
    assertEquals(10, it.advance(10));
    assertEquals(13, it.docIDRunEnd());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(13));
  }
}
