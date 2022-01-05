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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexOutputAlignment extends LuceneTestCase {

  public void testAlignmentCalculation() {
    assertEquals(0L, IndexOutput.alignOffset(0L, Long.BYTES));
    assertEquals(0L, IndexOutput.alignOffset(0L, Integer.BYTES));
    assertEquals(0L, IndexOutput.alignOffset(0L, Short.BYTES));
    assertEquals(0L, IndexOutput.alignOffset(0L, Byte.BYTES));

    assertEquals(8L, IndexOutput.alignOffset(1L, Long.BYTES));
    assertEquals(4L, IndexOutput.alignOffset(1L, Integer.BYTES));
    assertEquals(2L, IndexOutput.alignOffset(1L, Short.BYTES));
    assertEquals(1L, IndexOutput.alignOffset(1L, Byte.BYTES));

    assertEquals(32L, IndexOutput.alignOffset(25L, Long.BYTES));
    assertEquals(28L, IndexOutput.alignOffset(25L, Integer.BYTES));
    assertEquals(26L, IndexOutput.alignOffset(25L, Short.BYTES));
    assertEquals(25L, IndexOutput.alignOffset(25L, Byte.BYTES));

    final long val = 1L << 48;
    assertEquals(val, IndexOutput.alignOffset(val - 1, Long.BYTES));
    assertEquals(val, IndexOutput.alignOffset(val - 1, Integer.BYTES));
    assertEquals(val, IndexOutput.alignOffset(val - 1, Short.BYTES));
    // byte alignment never changes anything:
    assertEquals(val - 1, IndexOutput.alignOffset(val - 1, Byte.BYTES));

    assertEquals(Long.MAX_VALUE, IndexOutput.alignOffset(Long.MAX_VALUE, Byte.BYTES));
  }

  public void testInvalidAlignments() {
    assertInvalidAligment(0);
    assertInvalidAligment(-1);
    assertInvalidAligment(-2);
    assertInvalidAligment(6);
    assertInvalidAligment(43);
    assertInvalidAligment(Integer.MIN_VALUE);

    assertThrows(IllegalArgumentException.class, () -> IndexOutput.alignOffset(-1L, 1));
    assertThrows(ArithmeticException.class, () -> IndexOutput.alignOffset(Long.MAX_VALUE, 2));
  }

  private static void assertInvalidAligment(int size) {
    assertThrows(IllegalArgumentException.class, () -> IndexOutput.alignOffset(1L, size));
  }

  public void testOutputAlignment() throws IOException {
    IntStream.of(Long.BYTES, Integer.BYTES, Short.BYTES, Byte.BYTES)
        .forEach(TestIndexOutputAlignment::runTestOutputAlignment);
  }

  private static void runTestOutputAlignment(int alignment) {
    try (IndexOutput out =
        new OutputStreamIndexOutput("test output", "test", new ByteArrayOutputStream(), 8192)) {
      for (int i = 0; i < 10 * RANDOM_MULTIPLIER; i++) {
        // write some bytes
        int length = random().nextInt(32);
        out.writeBytes(new byte[length], length);
        long origPos = out.getFilePointer();
        // align to next boundary
        long newPos = out.alignFilePointer(alignment);
        assertEquals(out.getFilePointer(), newPos);
        assertTrue("not aligned", newPos % alignment == 0);
        assertTrue("newPos >=", newPos >= origPos);
        assertTrue("too much added", newPos - origPos < alignment);
      }
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }
  }
}
