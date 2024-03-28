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

import static org.apache.lucene.util.IntBlockPool.INT_BLOCK_SIZE;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IntBlockPool;

/** tests basic {@link IntBlockPool} functionality */
public class TestIntBlockPool extends LuceneTestCase {
  public void testWriteReadReset() {
    IntBlockPool pool = new IntBlockPool(new IntBlockPool.DirectAllocator());
    pool.nextBuffer();

    // Write <count> consecutive ints to the buffer, possibly allocating a new buffer
    int count = random().nextInt(2 * INT_BLOCK_SIZE);
    for (int i = 0; i < count; i++) {
      if (pool.intUpto == INT_BLOCK_SIZE) {
        pool.nextBuffer();
      }
      pool.buffer[pool.intUpto++] = i;
    }

    // Check that all the ints are present in th buffer pool
    for (int i = 0; i < count; i++) {
      assertEquals(i, pool.buffers[i / INT_BLOCK_SIZE][i % INT_BLOCK_SIZE]);
    }

    // Reset without filling with zeros and check that the first buffer still has the ints
    count = Math.min(count, INT_BLOCK_SIZE);
    pool.reset(false, true);
    for (int i = 0; i < count; i++) {
      assertEquals(i, pool.buffers[0][i]);
    }

    // Reset and fill with zeros, then check there is no data left
    pool.intUpto = count;
    pool.reset(true, true);
    for (int i = 0; i < count; i++) {
      assertEquals(0, pool.buffers[0][i]);
    }
  }

  public void testTooManyAllocs() {
    // Use a mock allocator that doesn't waste memory
    IntBlockPool pool =
        new IntBlockPool(
            new IntBlockPool.Allocator(0) {
              final int[] buffer = new int[0];

              @Override
              public void recycleIntBlocks(int[][] blocks, int start, int end) {}

              @Override
              public int[] getIntBlock() {
                return buffer;
              }
            });
    pool.nextBuffer();

    boolean throwsException = false;
    for (int i = 0; i < Integer.MAX_VALUE / INT_BLOCK_SIZE + 1; i++) {
      try {
        pool.nextBuffer();
      } catch (
          @SuppressWarnings("unused")
          ArithmeticException ignored) {
        // The offset overflows on the last attempt to call nextBuffer()
        throwsException = true;
        break;
      }
    }
    assertTrue(throwsException);
    assertTrue(pool.intOffset + INT_BLOCK_SIZE < pool.intOffset);
  }
}
