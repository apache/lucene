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
package org.apache.lucene.codecs.lucene99;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.util.function.BiFunction;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestGroupVInt extends LuceneTestCase {
  private static final BiFunction<String, ByteBuffersDataOutput, IndexInput>
      OUTPUT_AS_MANY_BUFFERS_ALLOWS_ARRAY =
          (fileName, output) -> {
            // set asReadOnlyBuffer = false to allow get array form ByteBuffer
            ByteBuffersDataInput dataInput = output.toDataInput(false);
            String inputName =
                String.format(
                    Locale.ROOT,
                    "%s (file=%s, buffers=%s)",
                    ByteBuffersIndexInput.class.getSimpleName(),
                    fileName,
                    dataInput.toString());
            return new ByteBuffersIndexInput(dataInput, inputName);
          };

  public void testAllDirectory() throws IOException {
    // NIOFSDirectory
    Directory dir = new NIOFSDirectory(Files.createTempDirectory("groupvintdata"));
    doTest(dir, 5, 1, 31, ForUtil.BLOCK_SIZE);
    dir.close();

    // MMapDirectory
    dir = new MMapDirectory(Files.createTempDirectory("groupvintdata"));
    doTest(dir, 5, 1, 31, ForUtil.BLOCK_SIZE);
    dir.close();

    // ByteBuffersDataOutput
    dir =
        new ByteBuffersDirectory(
            new SingleInstanceLockFactory(),
            ByteBuffersDataOutput::new,
            OUTPUT_AS_MANY_BUFFERS_ALLOWS_ARRAY);
    doTest(dir, 5, 1, 31, ForUtil.BLOCK_SIZE);
    dir.close();
  }

  public void testFallback() throws IOException {
    Directory dir = newFSDirectory(createTempDir());
    doTest(dir, 5, 1, 6, 8);
    dir.close();
  }

  public void testDataTypes() throws IOException {
    final long[] values = new long[] {43, 12345, 123456, 1234567890};
    Directory dir = newFSDirectory(createTempDir());
    IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
    out.writeByte((byte) 43);
    out.writeShort((short) 12345);
    out.writeInt(1234567890);
    out.writeGroupVInts(values, 4);
    out.writeLong(1234567890123456789L);
    out.close();

    long[] restored = new long[4];
    IndexInput in = dir.openInput("test", IOContext.DEFAULT);
    assertEquals(43, in.readByte());
    assertEquals(12345, in.readShort());
    assertEquals(1234567890, in.readInt());
    in.readGroupVInts(restored, 4);
    assertArrayEquals(values, restored);
    assertEquals(1234567890123456789L, in.readLong());
    in.close();
    dir.close();
  }

  public void testByteBuffersReadOnly() throws IOException {
    final boolean[] readShort = {false};
    Directory dir =
        new FilterDirectory(new ByteBuffersDirectory()) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            return new FilterIndexInput("wrapped", in.openInput(name, context)) {
              @Override
              public short readShort() throws IOException {
                readShort[0] = true;
                return this.in.readShort();
              }
            };
          }
        };
    doTest(dir, 1, 12, 14, 64);
    assertTrue(readShort[0]);
    dir.close();
  }

  public void testEncodeDecode() throws IOException {
    Directory dir = newFSDirectory(createTempDir());
    doTest(dir, atLeast(100), 1, 31, ForUtil.BLOCK_SIZE);
    dir.close();
  }

  public void doTest(Directory dir, int iterations, int minBpv, int maxBpv, int maxNumValues)
      throws IOException {
    long[] values = new long[maxNumValues];
    long[] restored = new long[maxNumValues];

    for (int i = 0; i < iterations; i++) {
      final int bpv = TestUtil.nextInt(random(), minBpv, maxBpv);
      final int numValues = TestUtil.nextInt(random(), 1, maxNumValues);

      // encode
      for (int j = 0; j < numValues; j++) {
        values[j] = RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
      }
      IndexOutput out = dir.createOutput("group-varint", IOContext.DEFAULT);
      out.writeGroupVInts(values, numValues);
      out.close();

      // decode
      IndexInput in = dir.openInput("group-varint", IOContext.DEFAULT);
      in.readGroupVInts(restored, numValues);
      in.close();
      assertArrayEquals(
          ArrayUtil.copyOfSubArray(values, 0, numValues),
          ArrayUtil.copyOfSubArray(restored, 0, numValues));
      dir.deleteFile("group-varint");
    }
  }
}
