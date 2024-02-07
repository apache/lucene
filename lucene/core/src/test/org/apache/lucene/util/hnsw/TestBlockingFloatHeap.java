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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

import java.util.concurrent.CountDownLatch;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;

public class TestBlockingFloatHeap extends LuceneTestCase {

  public void testBasicOperations() {
    BlockingFloatHeap heap = new BlockingFloatHeap(3);
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
    BlockingFloatHeap heap = new BlockingFloatHeap(size);
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

  @SuppressForbidden(reason = "Thread sleep")
  public void testMultipleThreads() throws Exception {
    Thread[] threads = new Thread[randomIntBetween(3, 20)];
    final CountDownLatch latch = new CountDownLatch(1);
    BlockingFloatHeap globalHeap = new BlockingFloatHeap(1);

    for (int i = 0; i < threads.length; i++) {
      threads[i] =
          new Thread(
              () -> {
                try {
                  latch.await();
                  int numIterations = randomIntBetween(10, 100);
                  float bottomValue = 0;

                  while (numIterations-- > 0) {
                    bottomValue += randomIntBetween(0, 5);
                    globalHeap.offer(bottomValue);
                    Thread.sleep(randomIntBetween(0, 50));

                    float globalBottomValue = globalHeap.peek();
                    assertTrue(globalBottomValue >= bottomValue);
                    bottomValue = globalBottomValue;
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      threads[i].start();
    }

    latch.countDown();
    for (Thread t : threads) {
      t.join();
    }
  }
}
