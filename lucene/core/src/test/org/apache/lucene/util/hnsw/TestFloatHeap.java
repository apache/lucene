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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFloatHeap extends LuceneTestCase {

  public void testBasicOperations() {
    FloatHeap heap = new FloatHeap(3);
    heap.offer(2);
    heap.offer(4);
    heap.offer(1);
    heap.offer(3);
    assertEquals(3, heap.size());
    assertEquals(2, heap.peek(), 0);

    assertEquals(2, heap.poll(), 0);
    assertEquals(3, heap.poll(), 0);
    assertEquals(4, heap.poll(), 0);
    assertEquals(0, heap.size(), 0);
  }

  public void testBasicOperations2() {
    int size = atLeast(10);
    FloatHeap heap = new FloatHeap(size);
    double sum = 0, sum2 = 0;

    for (int i = 0; i < size; i++) {
      float next = random().nextFloat();
      sum += next;
      heap.offer(next);
    }

    float last = Float.NEGATIVE_INFINITY;
    for (long i = 0; i < size; i++) {
      float next = heap.poll();
      assertTrue(next >= last);
      last = next;
      sum2 += last;
    }
    assertEquals(sum, sum2, 0.01);
  }

  public void testClear() {
    FloatHeap heap = new FloatHeap(3);
    heap.offer(20);
    heap.offer(40);
    heap.offer(30);
    assertEquals(3, heap.size());
    assertEquals(20, heap.peek(), 0);

    heap.clear();
    assertEquals(0, heap.size(), 0);
    assertEquals(20, heap.peek(), 0);

    heap.offer(15);
    heap.offer(35);
    assertEquals(2, heap.size());
    assertEquals(15, heap.peek(), 0);

    assertEquals(15, heap.poll(), 0);
    assertEquals(35, heap.poll(), 0);
    assertEquals(0, heap.size(), 0);
  }
}
