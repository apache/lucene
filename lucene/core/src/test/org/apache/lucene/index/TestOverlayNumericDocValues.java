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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestOverlayNumericDocValues extends LuceneTestCase {

  /**
   * Sparse array-backed {@link NumericDocValues} over ascending docs; used in one access mode per
   * instance.
   */
  private static NumericDocValues sparse(int[] docs, long[] values) {
    return new NumericDocValues() {
      int i = -1;
      int doc = -1;

      @Override
      public long longValue() {
        return values[i];
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int advance(int target) {
        i++;
        while (i < docs.length && docs[i] < target) {
          i++;
        }
        doc = i < docs.length ? docs[i] : DocIdSetIterator.NO_MORE_DOCS;
        return doc;
      }

      @Override
      public int nextDoc() {
        return advance(doc + 1);
      }

      @Override
      public boolean advanceExact(int target) {
        int j = Math.max(i, 0);
        while (j < docs.length && docs[j] < target) {
          j++;
        }
        i = j;
        doc = target;
        return i < docs.length && docs[i] == target;
      }

      @Override
      public long cost() {
        return docs.length;
      }
    };
  }

  /** base dense 0..9, an older delta on {2,5}, a newest delta on {5,7}: newest wins on doc 5. */
  public void testAdvanceExactNewestWins() throws IOException {
    NumericDocValues base =
        sparse(
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            new long[] {100, 101, 102, 103, 104, 105, 106, 107, 108, 109});
    NumericDocValues d1 = sparse(new int[] {2, 5}, new long[] {201, 205});
    NumericDocValues d2 = sparse(new int[] {5, 7}, new long[] {305, 307});
    long[] expected = {100, 101, 201, 103, 104, 305, 106, 307, 108, 109};

    OverlayNumericDocValues overlay =
        new OverlayNumericDocValues(new NumericDocValues[] {d2, d1, base});
    for (int doc = 0; doc < 10; doc++) {
      assertTrue("doc " + doc, overlay.advanceExact(doc));
      assertEquals("doc " + doc, expected[doc], overlay.longValue());
    }
  }

  public void testNextDocUnionMerge() throws IOException {
    NumericDocValues base =
        sparse(
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            new long[] {100, 101, 102, 103, 104, 105, 106, 107, 108, 109});
    NumericDocValues d1 = sparse(new int[] {2, 5}, new long[] {201, 205});
    NumericDocValues d2 = sparse(new int[] {5, 7}, new long[] {305, 307});
    long[] expected = {100, 101, 201, 103, 104, 305, 106, 307, 108, 109};

    OverlayNumericDocValues overlay =
        new OverlayNumericDocValues(new NumericDocValues[] {d2, d1, base});
    List<Integer> seen = new ArrayList<>();
    for (int doc = overlay.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = overlay.nextDoc()) {
      seen.add(doc);
      assertEquals("doc " + doc, expected[doc], overlay.longValue());
    }
    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), seen);
  }

  /** Sparse base: docs not covered by any layer have no value; the union still merges correctly. */
  public void testSparseBaseNextDocAndAdvanceExact() throws IOException {
    NumericDocValues base = sparse(new int[] {0, 4}, new long[] {100, 104});
    NumericDocValues d1 = sparse(new int[] {4, 6}, new long[] {204, 206}); // newest wins on doc 4
    OverlayNumericDocValues overlay =
        new OverlayNumericDocValues(new NumericDocValues[] {d1, base});

    List<Integer> seen = new ArrayList<>();
    for (int doc = overlay.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = overlay.nextDoc()) {
      seen.add(doc);
    }
    assertEquals(List.of(0, 4, 6), seen);

    OverlayNumericDocValues ra =
        new OverlayNumericDocValues(
            new NumericDocValues[] {
              sparse(new int[] {4, 6}, new long[] {204, 206}),
              sparse(new int[] {0, 4}, new long[] {100, 104})
            });
    assertTrue(ra.advanceExact(0));
    assertEquals(100, ra.longValue());
    assertFalse("doc 1 has no value", ra.advanceExact(1));
    assertTrue(ra.advanceExact(4));
    assertEquals(204, ra.longValue()); // newest layer wins
    assertFalse("doc 5 has no value", ra.advanceExact(5));
    assertTrue(ra.advanceExact(6));
    assertEquals(206, ra.longValue());
  }

  public void testAdvanceSkips() throws IOException {
    NumericDocValues base =
        sparse(
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            new long[] {100, 101, 102, 103, 104, 105, 106, 107, 108, 109});
    NumericDocValues d1 = sparse(new int[] {5}, new long[] {205});
    OverlayNumericDocValues overlay =
        new OverlayNumericDocValues(new NumericDocValues[] {d1, base});
    assertEquals(5, overlay.advance(5));
    assertEquals(205, overlay.longValue());
    assertEquals(6, overlay.nextDoc());
    assertEquals(106, overlay.longValue());
    assertEquals(9, overlay.advance(9));
    assertEquals(109, overlay.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, overlay.nextDoc());
  }
}
