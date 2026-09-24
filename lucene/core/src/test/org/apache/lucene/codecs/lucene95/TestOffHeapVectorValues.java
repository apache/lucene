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

package org.apache.lucene.codecs.lucene95;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestOffHeapVectorValues extends LuceneTestCase {

  public void testPrefetchVectorValuesWithMoreThanOneOrds() throws IOException {
    // {1, 2} are consecutive, so they are prefetched with a single read.
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    values.prefetch(new int[] {1, 2, 5}, 2);
    assertEquals(1, byteIndexInput.getPrefetchCount());

    CountingIndexInput floatIndexInput = new CountingIndexInput();
    OffHeapFloatVectorValues floatValues = createTestFloatVectorValues(floatIndexInput);
    floatValues.prefetch(new int[] {1, 2, 5}, 2);
    assertEquals(1, floatIndexInput.getPrefetchCount());
  }

  public void testPrefetchCoalescesConsecutiveOrdsOnly() throws IOException {
    // One read per run of consecutive ords: {1, 2, 3}, {7}, {9, 10}.
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    values.prefetch(new int[] {1, 2, 3, 7, 9, 10}, 6);
    assertEquals(3, byteIndexInput.getPrefetchCount());

    // Ords are not sorted here, so no run forms and every ord costs a read.
    CountingIndexInput floatIndexInput = new CountingIndexInput();
    OffHeapFloatVectorValues floatValues = createTestFloatVectorValues(floatIndexInput);
    floatValues.prefetch(new int[] {10, 3, 2, 8}, 4);
    assertEquals(4, floatIndexInput.getPrefetchCount());
  }

  public void testPrefetchContiguousRun() throws IOException {
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    assertEquals(4, values.prefetch(10, 4));
    assertEquals(1, byteIndexInput.getPrefetchCount());

    // A run reaching past the last ord is clamped to the vectors that exist.
    assertEquals(2, values.prefetch(98, 50));
    assertEquals(2, byteIndexInput.getPrefetchCount());

    // Nothing to prefetch: out of range ord, or a non-positive count.
    assertEquals(0, values.prefetch(100, 1));
    assertEquals(0, values.prefetch(-1, 1));
    assertEquals(0, values.prefetch(1, 0));
    assertEquals(2, byteIndexInput.getPrefetchCount());
  }

  public void testPrefetchReportsNothingPrefetchedWhenInputDeclinesIt() throws IOException {
    // The default IndexInput#prefetch is a no-op returning false: callers gain nothing by
    // deferring.
    OffHeapFloatVectorValues floatValues =
        createTestFloatVectorValues(new NoopPrefetchIndexInput());
    assertEquals(0, floatValues.prefetch(1, 4));
  }

  public void testPrefetchVectorValuesWithLessThanOneOrds() throws IOException {
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    values.prefetch(null, 0);
    assertEquals(0, byteIndexInput.getPrefetchCount());
    values.prefetch(new int[] {1}, 1);
    assertEquals(0, byteIndexInput.getPrefetchCount());

    CountingIndexInput floatIndexInput = new CountingIndexInput();
    OffHeapFloatVectorValues floatValues = createTestFloatVectorValues(floatIndexInput);
    floatValues.prefetch(null, 0);
    assertEquals(0, floatIndexInput.getPrefetchCount());
    floatValues.prefetch(new int[] {1}, 1);
    assertEquals(0, floatIndexInput.getPrefetchCount());
  }

  public void testPrefetchBoundedByArrayLength() throws IOException {
    // numOrds may exceed the length of the (reused) scratch array. Prefetch must be bounded by the
    // array length so it only prefetches the available ords instead of running past the array end.
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    values.prefetch(new int[] {1, 3}, 5);
    assertEquals(2, byteIndexInput.getPrefetchCount());

    CountingIndexInput floatIndexInput = new CountingIndexInput();
    OffHeapFloatVectorValues floatValues = createTestFloatVectorValues(floatIndexInput);
    floatValues.prefetch(new int[] {1, 3}, 5);
    assertEquals(2, floatIndexInput.getPrefetchCount());
  }

  private OffHeapByteVectorValues createTestByteVectorValues(IndexInput indexInput)
      throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        100, 100, indexInput, 1, null, VectorSimilarityFunction.EUCLIDEAN);
  }

  private OffHeapFloatVectorValues createTestFloatVectorValues(IndexInput indexInput)
      throws IOException {
    return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
        100, 100, indexInput, 4, null, VectorSimilarityFunction.EUCLIDEAN);
  }

  /** An input that declines to prefetch, like the default {@link IndexInput#prefetch}. */
  private static final class NoopPrefetchIndexInput extends CountingIndexInput {

    @Override
    public boolean prefetch(long offset, long length) throws IOException {
      return false;
    }
  }

  private static class CountingIndexInput extends IndexInput {

    AtomicInteger counter;

    public CountingIndexInput() {
      super("closing index input");
      counter = new AtomicInteger(0);
    }

    @Override
    public boolean prefetch(long offset, long length) throws IOException {
      counter.incrementAndGet();
      return true;
    }

    public int getPrefetchCount() {
      return counter.get();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
      return 0;
    }

    @Override
    public void seek(long pos) throws IOException {}

    @Override
    public long length() {
      return 0;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return null;
    }

    @Override
    public byte readByte() throws IOException {
      return 0;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {}
  }
}
