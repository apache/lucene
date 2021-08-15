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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.util.LuceneTestCase;

public class TestValuesDISI extends LuceneTestCase {

  public void testBasic() throws Exception {
    int[] leadDocs = {1, 2, 3, 4, 5};
    Map<Integer, Long> expectedValues1 =
        Map.of(
            2, 100L,
            3, 11L,
            5, 27L);
    Map<Integer, Long> expectedValues2 =
        Map.of(
            2, 210L,
            5, 20L);
    List<Map<Integer, Long>> expectedValues = List.of(expectedValues1, expectedValues2);

    DocIdSetIterator lead = createLead(leadDocs);
    LongValues values1 = createLongValues(expectedValues1);
    LongValues values2 = createLongValues(expectedValues2);
    List<LongValues> values = List.of(values1, values2);

    DocIdSetIterator conjunction = ConjunctionUtils.createValuesConjunction(lead, values);
    checkValuesConjunction(leadDocs, expectedValues, conjunction, values);
  }

  public void testEmptyValues() throws Exception {
    int[] leadDocs = {1, 2, 3, 4, 5};

    DocIdSetIterator lead = createLead(leadDocs);

    DocIdSetIterator conjunction = ConjunctionUtils.createValuesConjunction(lead, List.of());
    checkValuesConjunction(leadDocs, new ArrayList<>(), conjunction, List.of());
  }

  public void testRandom() throws Exception {
    int iterations = random().nextInt(100);
    for (int iter = 0; iter < iterations; iter++) {
      int[] leadDocs = randomDocs(random().nextInt(10000));
      DocIdSetIterator lead = createLead(leadDocs);

      int numValues = random().nextInt(10);
      List<Map<Integer, Long>> expectedValues = new ArrayList<>();
      List<LongValues> values = new ArrayList<>();
      for (int i = 0; i < numValues; i++) {
        Map<Integer, Long> expected = new HashMap<>();
        expectedValues.add(expected);
        for (int doc = 0; doc < leadDocs.length; doc++) {
          if (random().nextInt(10) < 8) {
            expected.put(doc, random().nextLong());
          }
        }
        values.add(createLongValues(expected));
      }

      DocIdSetIterator conjunction = ConjunctionUtils.createValuesConjunction(lead, values);
      checkValuesConjunction(leadDocs, expectedValues, conjunction, values);
    }
  }

  private void checkValuesConjunction(
      int[] leadDocs,
      List<Map<Integer, Long>> expectedValues,
      DocIdSetIterator conjunction,
      List<LongValues> values)
      throws IOException {
    int currDoc = conjunction.nextDoc();
    for (int expectedDoc : leadDocs) {
      boolean shouldFind = true;
      for (Map<Integer, Long> expected : expectedValues) {
        if (expected.containsKey(expectedDoc) == false) {
          shouldFind = false;
          break;
        }
      }
      if (shouldFind == false) {
        currDoc = conjunction.advance(expectedDoc);
        assertTrue(currDoc > expectedDoc);
      } else {
        if (random().nextBoolean()) {
          currDoc = conjunction.advance(expectedDoc);
        } else if (expectedDoc != currDoc) {
          currDoc = conjunction.nextDoc();
        }
        assertEquals(expectedDoc, currDoc);
        for (int i = 0; i < values.size(); i++) {
          assertEquals((long) expectedValues.get(i).get(currDoc), values.get(i).longValue());
        }
      }
    }
  }

  private int[] randomDocs(int count) {
    int[] result = new int[count];
    for (int i = 0; i < count; i++) {
      result[i] = RandomNumbers.randomIntBetween(random(), 0, DocIdSetIterator.NO_MORE_DOCS - 1);
    }
    Arrays.sort(result);

    return result;
  }

  private DocIdSetIterator createLead(int[] values) {
    return new DocIdSetIterator() {
      private int i = -1; // positioned on current doc

      @Override
      public int docID() {
        return values[i];
      }

      @Override
      public int nextDoc() {
        if (i == values.length - 1) {
          return NO_MORE_DOCS;
        }
        return values[++i];
      }

      @Override
      public int advance(int target) {
        for (; i < values.length; i++) {
          if (values[i] >= target) {
            break;
          }
        }

        if (i == values.length) {
          return NO_MORE_DOCS;
        }

        return values[i];
      }

      @Override
      public long cost() {
        return values.length;
      }
    };
  }

  private LongValues createLongValues(Map<Integer, Long> expectedValues) {
    return new LongValues() {
      long currentVal;

      @Override
      public long longValue() {
        return currentVal;
      }

      @Override
      public boolean advanceExact(int doc) {
        if (expectedValues.containsKey(doc)) {
          currentVal = expectedValues.get(doc);
          return true;
        }
        return false;
      }
    };
  }
}
