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
package org.apache.lucene.util.bkd;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestBKDUtil extends LuceneTestCase {

  public void testEquals4() {
    int aOffset = TestUtil.nextInt(random(), 0, 3);
    byte[] a = new byte[Integer.BYTES + aOffset];
    int bOffset = TestUtil.nextInt(random(), 0, 3);
    byte[] b = new byte[Integer.BYTES + bOffset];

    for (int i = 0; i < Integer.BYTES; ++i) {
      a[aOffset + i] = (byte) random().nextInt(1 << 8);
    }
    System.arraycopy(a, aOffset, b, bOffset, 4);

    assertTrue(BKDUtil.equals4(a, aOffset, b, bOffset));

    for (int i = 0; i < Integer.BYTES; ++i) {
      do {
        b[bOffset + i] = (byte) random().nextInt(1 << 8);
      } while (b[bOffset + i] == a[aOffset + i]);

      assertFalse(BKDUtil.equals4(a, aOffset, b, bOffset));

      b[bOffset + i] = a[aOffset + i];
    }
  }

  public void testEquals8() {
    int aOffset = TestUtil.nextInt(random(), 0, 7);
    byte[] a = new byte[Long.BYTES + aOffset];
    int bOffset = TestUtil.nextInt(random(), 0, 7);
    byte[] b = new byte[Long.BYTES + bOffset];

    for (int i = 0; i < Long.BYTES; ++i) {
      a[aOffset + i] = (byte) random().nextInt(1 << 8);
    }
    System.arraycopy(a, aOffset, b, bOffset, 8);

    assertTrue(BKDUtil.equals8(a, aOffset, b, bOffset));

    for (int i = 0; i < Long.BYTES; ++i) {
      do {
        b[bOffset + i] = (byte) random().nextInt(1 << 8);
      } while (b[bOffset + i] == a[aOffset + i]);

      assertFalse(BKDUtil.equals8(a, aOffset, b, bOffset));

      b[bOffset + i] = a[aOffset + i];
    }
  }

  public void testCommonPrefixLength4() {
    int aOffset = TestUtil.nextInt(random(), 0, 3);
    byte[] a = new byte[Integer.BYTES + aOffset];
    int bOffset = TestUtil.nextInt(random(), 0, 3);
    byte[] b = new byte[Integer.BYTES + bOffset];

    for (int i = 0; i < Integer.BYTES; ++i) {
      a[aOffset + i] = (byte) random().nextInt(1 << 8);
      do {
        b[bOffset + i] = (byte) random().nextInt(1 << 8);
      } while (b[bOffset + i] == a[aOffset + i]);
    }

    for (int i = 0; i < Integer.BYTES; ++i) {
      assertEquals(i, BKDUtil.commonPrefixLength4(a, aOffset, b, bOffset));
      b[bOffset + i] = a[aOffset + i];
    }

    assertEquals(4, BKDUtil.commonPrefixLength4(a, aOffset, b, bOffset));
  }

  public void testCommonPrefixLength8() {
    int aOffset = TestUtil.nextInt(random(), 0, 7);
    byte[] a = new byte[Long.BYTES + aOffset];
    int bOffset = TestUtil.nextInt(random(), 0, 7);
    byte[] b = new byte[Long.BYTES + bOffset];

    for (int i = 0; i < Long.BYTES; ++i) {
      a[aOffset + i] = (byte) random().nextInt(1 << 8);
      do {
        b[bOffset + i] = (byte) random().nextInt(1 << 8);
      } while (b[bOffset + i] == a[aOffset + i]);
    }

    for (int i = 0; i < Long.BYTES; ++i) {
      assertEquals(i, BKDUtil.commonPrefixLength8(a, aOffset, b, bOffset));
      b[bOffset + i] = a[aOffset + i];
    }

    assertEquals(8, BKDUtil.commonPrefixLength8(a, aOffset, b, bOffset));
  }

  public void testCommonPrefixLengthN() {
    final int numBytes = TestUtil.nextInt(random(), 2, 16);

    int aOffset = TestUtil.nextInt(random(), 0, numBytes - 1);
    byte[] a = new byte[numBytes + aOffset];
    int bOffset = TestUtil.nextInt(random(), 0, numBytes - 1);
    byte[] b = new byte[numBytes + bOffset];

    for (int i = 0; i < numBytes; ++i) {
      a[aOffset + i] = (byte) random().nextInt(1 << 8);
      do {
        b[bOffset + i] = (byte) random().nextInt(1 << 8);
      } while (b[bOffset + i] == a[aOffset + i]);
    }

    for (int i = 0; i < numBytes; ++i) {
      assertEquals(i, BKDUtil.commonPrefixLengthN(a, aOffset, b, bOffset, numBytes));
      b[bOffset + i] = a[aOffset + i];
    }

    assertEquals(numBytes, BKDUtil.commonPrefixLengthN(a, aOffset, b, bOffset, numBytes));
  }
}
