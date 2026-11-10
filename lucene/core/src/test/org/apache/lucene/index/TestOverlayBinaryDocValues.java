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
import org.apache.lucene.util.BytesRef;

public class TestOverlayBinaryDocValues extends LuceneTestCase {

  private static BinaryDocValues sparse(int[] docs, String[] values) {
    return new BinaryDocValues() {
      int i = -1;
      int doc = -1;

      @Override
      public BytesRef binaryValue() {
        return new BytesRef(values[i]);
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

  public void testAdvanceExactNewestWins() throws IOException {
    BinaryDocValues base = sparse(new int[] {0, 1, 2, 3}, new String[] {"a0", "a1", "a2", "a3"});
    BinaryDocValues d1 = sparse(new int[] {1, 2}, new String[] {"b1", "b2"});
    BinaryDocValues d2 = sparse(new int[] {2}, new String[] {"c2"});
    String[] expected = {"a0", "b1", "c2", "a3"};

    OverlayBinaryDocValues overlay =
        new OverlayBinaryDocValues(new BinaryDocValues[] {d2, d1, base});
    for (int doc = 0; doc < 4; doc++) {
      assertTrue("doc " + doc, overlay.advanceExact(doc));
      assertEquals("doc " + doc, new BytesRef(expected[doc]), overlay.binaryValue());
    }
  }

  public void testNextDocUnionMerge() throws IOException {
    BinaryDocValues base = sparse(new int[] {0, 4}, new String[] {"a0", "a4"});
    BinaryDocValues d1 =
        sparse(new int[] {4, 6}, new String[] {"b4", "b6"}); // newest wins on doc 4
    OverlayBinaryDocValues overlay = new OverlayBinaryDocValues(new BinaryDocValues[] {d1, base});

    List<Integer> seen = new ArrayList<>();
    List<String> vals = new ArrayList<>();
    for (int doc = overlay.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = overlay.nextDoc()) {
      seen.add(doc);
      vals.add(overlay.binaryValue().utf8ToString());
    }
    assertEquals(List.of(0, 4, 6), seen);
    assertEquals(List.of("a0", "b4", "b6"), vals);
  }
}
