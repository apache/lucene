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
package org.apache.lucene.store;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBytesOfLength;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLongBetween;

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOConsumer;
import org.junit.Test;

public final class TestByteBuffersDataInput extends LuceneTestCase {
  @Test
  public void testSanity() throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    ByteBuffersDataInput o1 = out.toDataInput();
    assertEquals(0, o1.length());
    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          o1.readByte();
        });

    out.writeByte((byte) 1);

    ByteBuffersDataInput o2 = out.toDataInput();
    assertEquals(1, o2.length());
    assertEquals(0, o2.position());
    assertEquals(0, o1.length());

    assertTrue(o2.ramBytesUsed() > 0);
    assertEquals(1, o2.readByte());
    assertEquals(1, o2.position());
    assertEquals(1, o2.readByte(0));

    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          o2.readByte();
        });

    assertEquals(1, o2.position());
  }

  @Test
  public void testRandomReads() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

    int max = LuceneTestCase.TEST_NIGHTLY ? 1_000_000 : 100_000;
    List<IOConsumer<DataInput>> reply =
        TestByteBuffersDataOutput.addRandomData(dst, nonAssertingRandom(random()), max);

    ByteBuffersDataInput src = dst.toDataInput();
    for (IOConsumer<DataInput> c : reply) {
      c.accept(src);
    }

    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          src.readByte();
        });
  }

  @Test
  public void testRandomReadsOnSlices() throws Exception {
    for (int reps = randomIntBetween(1, 20); --reps > 0; ) {
      ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

      byte[] prefix = new byte[randomIntBetween(0, 1024 * 8)];
      dst.writeBytes(prefix);

      int max = atLeast(5000);
      List<IOConsumer<DataInput>> reply =
          TestByteBuffersDataOutput.addRandomData(dst, nonAssertingRandom(random()), max);

      byte[] suffix = new byte[randomIntBetween(0, 1024 * 8)];
      dst.writeBytes(suffix);

      ByteBuffersDataInput src =
          dst.toDataInput().slice(prefix.length, dst.size() - prefix.length - suffix.length);

      assertEquals(0, src.position());
      assertEquals(dst.size() - prefix.length - suffix.length, src.length());
      for (IOConsumer<DataInput> c : reply) {
        c.accept(src);
      }

      LuceneTestCase.expectThrows(
          EOFException.class,
          () -> {
            src.readByte();
          });
    }
  }

  @Test
  public void testSeekEmpty() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    ByteBuffersDataInput in = dst.toDataInput();
    in.seek(0);

    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          in.seek(1);
        });

    in.seek(0);
    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          in.readByte();
        });
  }

  @Test
  public void testSeekAndSkip() throws Exception {
    for (int reps = randomIntBetween(1, 200); --reps > 0; ) {
      ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

      byte[] prefix = {};
      if (randomBoolean()) {
        prefix = new byte[randomIntBetween(1, 1024 * 8)];
        dst.writeBytes(prefix);
      }

      int max = 1000;
      List<IOConsumer<DataInput>> reply =
          TestByteBuffersDataOutput.addRandomData(dst, random(), max);

      ByteBuffersDataInput in = dst.toDataInput().slice(prefix.length, dst.size() - prefix.length);

      in.seek(0);
      for (IOConsumer<DataInput> c : reply) {
        c.accept(in);
      }

      in.seek(0);
      for (IOConsumer<DataInput> c : reply) {
        c.accept(in);
      }

      byte[] array = dst.toArrayCopy();
      array = ArrayUtil.copyOfSubArray(array, prefix.length, array.length);

      // test seeking
      for (int i = 0; i < 1000; i++) {
        int offs = randomIntBetween(0, array.length - 1);
        in.seek(offs);
        assertEquals(offs, in.position());
        assertEquals(array[offs], in.readByte());
      }

      // test skipping
      int maxSkipTo = array.length - 1;
      in.seek(0);
      // skip chunks of bytes until exhausted
      for (int curr = 0; curr < maxSkipTo; ) {
        int skipTo = randomIntBetween(curr, maxSkipTo);
        int step = skipTo - curr;
        in.skipBytes(step);
        assertEquals(array[skipTo], in.readByte());
        curr = skipTo + 1; // +1 for read byte
      }

      in.seek(in.length());
      assertEquals(in.length(), in.position());
      LuceneTestCase.expectThrows(
          EOFException.class,
          () -> {
            in.readByte();
          });
    }
  }

  @Test
  public void testSlicingWindow() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    assertEquals(0, dst.toDataInput().slice(0, 0).length());

    dst.writeBytes(randomBytesOfLength(1024 * 8));
    ByteBuffersDataInput in = dst.toDataInput();
    for (int offset = 0, max = (int) dst.size(); offset < max; offset++) {
      assertEquals(0, in.slice(offset, 0).length());
      assertEquals(1, in.slice(offset, 1).length());

      int window = Math.min(max - offset, 1024);
      assertEquals(window, in.slice(offset, window).length());
    }
    assertEquals(0, in.slice((int) dst.size(), 0).length());
  }

  @Test
  @Timeout(millis = 5000)
  public void testEofOnArrayReadPastBufferSize() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    dst.writeBytes(new byte[10]);

    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          ByteBuffersDataInput in = dst.toDataInput();
          in.readBytes(new byte[100], 0, 100);
        });

    LuceneTestCase.expectThrows(
        EOFException.class,
        () -> {
          ByteBuffersDataInput in = dst.toDataInput();
          in.readBytes(ByteBuffer.allocate(100), 100);
        });
  }

  // https://issues.apache.org/jira/browse/LUCENE-8625
  @Test
  public void testSlicingLargeBuffers() throws IOException {
    // Simulate a "large" (> 4GB) input by duplicating
    // buffers with the same content.
    int MB = 1024 * 1024;
    byte[] pageBytes = randomBytesOfLength(4 * MB);
    ByteBuffer page = ByteBuffer.wrap(pageBytes);

    // Add some head shift on the first buffer.
    final int shift = randomIntBetween(0, pageBytes.length / 2);

    final long simulatedLength = randomLongBetween(0, 2018) + 4L * Integer.MAX_VALUE;

    List<ByteBuffer> buffers = new ArrayList<>();
    long remaining = simulatedLength + shift;
    while (remaining > 0) {
      ByteBuffer bb = page.duplicate();
      if (bb.remaining() > remaining) {
        bb.limit(Math.toIntExact(bb.position() + remaining));
      }
      buffers.add(bb);
      remaining -= bb.remaining();
    }
    buffers.get(0).position(shift);

    ByteBuffersDataInput dst = new ByteBuffersDataInput(buffers);
    assertEquals(simulatedLength, dst.length());

    final long max = dst.length();
    long offset = 0;
    for (; offset < max; offset += randomIntBetween(MB, 4 * MB)) {
      assertEquals(0, dst.slice(offset, 0).length());
      assertEquals(1, dst.slice(offset, 1).length());

      long window = Math.min(max - offset, 1024);
      ByteBuffersDataInput slice = dst.slice(offset, window);
      assertEquals(window, slice.length());

      // Sanity check of the content against original pages.
      for (int i = 0; i < window; i++) {
        byte expected = pageBytes[(int) ((shift + offset + i) % pageBytes.length)];
        assertEquals(expected, slice.readByte(i));
      }
    }
  }
}
