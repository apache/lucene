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
package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestGroupVIntUtil extends LuceneTestCase {

  public void testLongArrayRoundTrip() throws IOException {
    long[] original = {1L, 127L, 128L, 16383L, 16384L, 2097151L, 2097152L, 268435455L};

    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    GroupVIntUtil.writeGroupVInts(out, original, original.length);

    ByteBuffersDataInput in = out.toDataInput();
    long[] result = new long[original.length];
    GroupVIntUtil.readGroupVInts(in, result, original.length);

    assertArrayEquals(original, result);
  }

  public void testSingleGroupVInt() throws IOException {
    long[] original = {1L, 2L, 3L, 4L};

    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    byte[] scratch = new byte[GroupVIntUtil.MAX_LENGTH_PER_GROUP];
    GroupVIntUtil.writeGroupVInts(out, scratch, original, original.length);

    ByteBuffersDataInput in = out.toDataInput();
    long[] result = new long[original.length];
    GroupVIntUtil.readGroupVInt(in, result, 0);

    assertArrayEquals(original, result);
  }

  public void testGroupVIntOverflow() throws IOException {
    try (Directory dir = newDirectory()) {
      final int size = 32;
      final long[] values = new long[size];
      final long[] restore = new long[size];
      values[0] = 1L << 31; // 2147483648 as long, but -2147483648 if interpreted as int.

      for (int i = 0; i < size; i++) {
        if (random().nextBoolean()) {
          values[i] = values[0];
        }
      }

      final int limit = random().nextInt(size) + 1;
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        GroupVIntUtil.writeGroupVInts(out, values, limit);
      }
      try (IndexInput in = dir.openInput("test", IOContext.DEFAULT)) {
        GroupVIntUtil.readGroupVInts(in, restore, limit);
        for (int i = 0; i < limit; i++) {
          assertEquals(values[i], restore[i]);
        }
      }

      values[0] = 0xFFFFFFFFL + 1;
      try (IndexOutput out = dir.createOutput("overflow", IOContext.DEFAULT)) {
        assertThrows(
            ArithmeticException.class, () -> GroupVIntUtil.writeGroupVInts(out, values, 4));
      }
    }
  }
}
