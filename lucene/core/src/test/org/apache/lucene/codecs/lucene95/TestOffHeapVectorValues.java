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
    CountingIndexInput byteIndexInput = new CountingIndexInput();
    OffHeapByteVectorValues values = createTestByteVectorValues(byteIndexInput);
    values.prefetch(new int[] {1, 2, 5}, 2);
    assertEquals(2, byteIndexInput.getPrefetchCount());

    CountingIndexInput floatIndexInput = new CountingIndexInput();
    OffHeapFloatVectorValues floatValues = createTestFloatVectorValues(floatIndexInput);
    floatValues.prefetch(new int[] {1, 2, 5}, 2);
    assertEquals(2, floatIndexInput.getPrefetchCount());
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

  private static final class CountingIndexInput extends IndexInput {

    AtomicInteger counter;

    public CountingIndexInput() {
      super("closing index input");
      counter = new AtomicInteger(0);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
      counter.incrementAndGet();
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
