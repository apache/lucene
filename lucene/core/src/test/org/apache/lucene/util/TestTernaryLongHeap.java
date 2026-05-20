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
package org.apache.lucene.util;

import java.util.ArrayList;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestTernaryLongHeap extends LuceneTestCase {

  private static void checkValidity(TernaryLongHeap heap) {
    long[] heapArray = heap.getHeapArray();
    int d = 3;
    int size = heap.size();
    for (int parent = 1; parent <= size; parent++) {
      int firstChild = d * (parent - 1) + 2;
      int lastChild = Math.min(firstChild + d - 1, size);
      for (int c = firstChild; c <= lastChild; c++) {
        assert heapArray[parent] <= heapArray[c];
      }
    }
  }

  public void testPQ() {
    testPQ(atLeast(10000), random());
  }

  public static void testPQ(int count, Random gen) {
    TernaryLongHeap pq = new TernaryLongHeap(count);
    long sum = 0, sum2 = 0;

    for (int i = 0; i < count; i++) {
      long next = gen.nextLong();
      sum += next;
      pq.push(next);
    }

    long last = Long.MIN_VALUE;
    for (long i = 0; i < count; i++) {
      long next = pq.pop();
      assertTrue(next >= last);
      last = next;
      sum2 += last;
    }

    assertEquals(sum, sum2);
  }

  public void testClear() {
    TernaryLongHeap pq = new TernaryLongHeap(3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(3, pq.size());
    pq.clear();
    assertEquals(0, pq.size());
  }

  public void testExceedBounds() {
    TernaryLongHeap pq = new TernaryLongHeap(1);
    pq.push(2);
    pq.push(0);
    assertEquals(2, pq.size()); // the heap has been extended to a new max size
    assertEquals(0, pq.top());
  }

  public void testFixedSize() {
    TernaryLongHeap pq = new TernaryLongHeap(3);
    pq.insertWithOverflow(2);
    pq.insertWithOverflow(3);
    pq.insertWithOverflow(1);
    pq.insertWithOverflow(5);
    pq.insertWithOverflow(7);
    pq.insertWithOverflow(1);
    assertEquals(3, pq.size());
    assertEquals(3, pq.top());
  }

  public void testDuplicateValues() {
    TernaryLongHeap pq = new TernaryLongHeap(3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(1, pq.top());
    pq.updateTop(3);
    assertEquals(3, pq.size());
    assertArrayEquals(new long[] {0, 2, 3, 3}, pq.getHeapArray());
  }

  public void testInsertions() {
    Random random = random();
    int numDocsInPQ = TestUtil.nextInt(random, 1, 100);
    TernaryLongHeap pq = new TernaryLongHeap(numDocsInPQ);
    Long lastLeast = null;

    // Basic insertion of new content
    ArrayList<Long> sds = new ArrayList<>(numDocsInPQ);
    for (int i = 0; i < numDocsInPQ * 10; i++) {
      long newEntry = Math.abs(random.nextLong());
      sds.add(newEntry);
      pq.insertWithOverflow(newEntry);
      checkValidity(pq);
      long newLeast = pq.top();
      if ((lastLeast != null) && (newLeast != newEntry) && (newLeast != lastLeast)) {
        // If there has been a change of least entry and it wasn't our new
        // addition we expect the scores to increase
        assertTrue(newLeast <= newEntry);
        assertTrue(newLeast >= lastLeast);
      }
      lastLeast = newLeast;
    }
  }

  public void testInvalid() {
    expectThrows(IllegalArgumentException.class, () -> new TernaryLongHeap(-1));
    expectThrows(IllegalArgumentException.class, () -> new TernaryLongHeap(0));
    expectThrows(
        IllegalArgumentException.class, () -> new TernaryLongHeap(ArrayUtil.MAX_ARRAY_LENGTH));
  }

  public void testUnbounded() {
    int initialSize = random().nextInt(10) + 1;
    TernaryLongHeap pq = new TernaryLongHeap(initialSize);
    int num = random().nextInt(100) + 1;
    long maxValue = Long.MIN_VALUE;
    int count = 0;
    for (int i = 0; i < num; i++) {
      long value = random().nextLong();
      if (random().nextBoolean()) {
        pq.push(value);
        count++;
      } else {
        boolean full = pq.size() >= initialSize;
        if (pq.insertWithOverflow(value)) {
          if (full == false) {
            count++;
          }
        }
      }
      maxValue = Math.max(maxValue, value);
    }
    assertEquals(count, pq.size());
    long last = Long.MIN_VALUE;
    while (pq.size() > 0) {
      long top = pq.top();
      long next = pq.pop();
      assertEquals(top, next);
      --count;
      assertTrue(next >= last);
      last = next;
    }
    assertEquals(0, count);
    assertEquals(maxValue, last);
  }
}
