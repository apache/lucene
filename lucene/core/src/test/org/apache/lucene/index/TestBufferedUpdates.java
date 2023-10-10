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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Unit test for {@link BufferedUpdates} */
public class TestBufferedUpdates extends LuceneTestCase {

  public void testRamBytesUsed() {
    BufferedUpdates bu = new BufferedUpdates("seg1");
    assertEquals(bu.ramBytesUsed(), 0L);
    assertFalse(bu.any());
    int queries = atLeast(1);
    for (int i = 0; i < queries; i++) {
      final int docIDUpto = random().nextBoolean() ? Integer.MAX_VALUE : random().nextInt(100000);
      final Term term = new Term("id", Integer.toString(random().nextInt(100)));
      bu.addQuery(new TermQuery(term), docIDUpto);
    }

    int terms = atLeast(1);
    for (int i = 0; i < terms; i++) {
      final int docIDUpto = random().nextBoolean() ? Integer.MAX_VALUE : random().nextInt(100000);
      final Term term = new Term("id", Integer.toString(random().nextInt(100)));
      bu.addTerm(term, docIDUpto);
    }
    assertTrue("we have added tons of docIds, terms and queries", bu.any());

    long totalUsed = bu.ramBytesUsed();
    assertTrue(totalUsed > 0);

    bu.clearDeleteTerms();
    assertTrue("only terms and docIds are cleaned, the queries are still in memory", bu.any());
    assertTrue("terms are cleaned, ram in used should decrease", totalUsed > bu.ramBytesUsed());

    bu.clear();
    assertFalse(bu.any());
    assertEquals(bu.ramBytesUsed(), 0L);
  }

  public void testDeletedTerms() {
    int iters = atLeast(10);
    String[] fields = new String[] {"a", "b", "c"};
    BufferedUpdates.DeletedTerms actual = new BufferedUpdates.DeletedTerms();
    for (int iter = 0; iter < iters; iter++) {

      Map<Term, Integer> expected = new HashMap<>();
      assertTrue(actual.isEmpty());

      int termCount = atLeast(5000);
      int maxBytesNum = random().nextInt(3) + 1;
      for (int i = 0; i < termCount; i++) {
        int byteNum = random().nextInt(maxBytesNum) + 1;
        byte[] bytes = new byte[byteNum];
        random().nextBytes(bytes);
        Term term = new Term(fields[random().nextInt(fields.length)], new BytesRef(bytes));
        int value = random().nextInt(10000000);
        expected.put(term, value);
        actual.put(term, value);
      }

      assertEquals(expected.size(), actual.size());

      for (Map.Entry<Term, Integer> entry : expected.entrySet()) {
        assertEquals(entry.getValue(), Integer.valueOf(actual.get(entry.getKey())));
      }

      List<Map.Entry<Term, Integer>> expectedSorted =
          expected.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .collect(Collectors.toList());
      List<Map.Entry<Term, Integer>> actualSorted = new ArrayList<>();
      actual.forEachOrdered(
          ((term, docId) -> {
            Term copy = new Term(term.field, BytesRef.deepCopyOf(term.bytes));
            actualSorted.add(Map.entry(copy, docId));
          }));

      assertEquals(expectedSorted, actualSorted);

      actual.clear();
      assertEquals(0, actual.size());
      assertEquals(0, actual.ramBytesUsed());
      assertNull(actual.getPool().buffer);
    }
  }
}
