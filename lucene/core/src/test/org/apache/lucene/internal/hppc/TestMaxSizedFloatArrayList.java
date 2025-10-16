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

package org.apache.lucene.internal.hppc;

import org.junit.Test;

/** Tests for {@link MaxSizedFloatArrayList}. */
public class TestMaxSizedFloatArrayList extends TestFloatArrayList {
  private static final int MAX_SIZE = 5;

  @Test
  public void testMaxSizeConstructor() {
    // Test valid maxSize
    MaxSizedFloatArrayList list = new MaxSizedFloatArrayList(MAX_SIZE, 3);
    assertEquals(3, list.buffer.length);
    assertEquals(MAX_SIZE, list.maxSize);

    // Test invalid maxSize (expectedElements > maxSize)
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(
          AssertionError.class,
          () -> {
            new MaxSizedFloatArrayList(2, 3);
          });
    }
  }

  @Test
  public void testMaxSizeLimit() {
    MaxSizedFloatArrayList list = new MaxSizedFloatArrayList(MAX_SIZE);

    // Fill up to maxSize
    for (int i = 0; i < MAX_SIZE; i++) {
      list.add(i + 0.5f);
    }
    assertEquals(MAX_SIZE, list.size());

    // Try to add beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.add(MAX_SIZE + 0.5f);
        });

    // Try to add array beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.add(new float[] {1.5f, 2.5f, 3.5f});
        });

    // Try to addAll beyond maxSize
    FloatArrayList otherList = new FloatArrayList();
    otherList.add(1.5f);
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.addAll(otherList);
        });
  }

  @Test
  public void testMaxSizeWithInsert() {
    MaxSizedFloatArrayList list = new MaxSizedFloatArrayList(MAX_SIZE);

    // Fill up to maxSize - 1
    for (int i = 0; i < MAX_SIZE - 1; i++) {
      list.add(i + 0.5f);
    }
    assertEquals(MAX_SIZE - 1, list.size());

    // Insert at the end (should succeed)
    list.insert(MAX_SIZE - 1, (MAX_SIZE - 1) + 0.5f);
    assertEquals(MAX_SIZE, list.size());

    // Try to insert beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.insert(0, MAX_SIZE + 0.5f);
        });
  }

  @Test
  public void testMaxSizeWithResize() {
    MaxSizedFloatArrayList list = new MaxSizedFloatArrayList(MAX_SIZE);

    // Try to resize beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.resize(MAX_SIZE + 1);
        });

    // Resize to maxSize (should succeed)
    list.resize(MAX_SIZE);
    assertEquals(MAX_SIZE, list.size());
  }

  @Test
  public void testMaxSizeWithEnsureCapacity() {
    MaxSizedFloatArrayList list = new MaxSizedFloatArrayList(MAX_SIZE);

    // Try to ensure capacity beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.ensureCapacity(MAX_SIZE + 1);
        });

    // Ensure capacity to maxSize (should succeed)
    list.ensureCapacity(MAX_SIZE);
    // Buffer can be larger than maxSize due to growth strategy, but not smaller if capacity is
    // ensured to maxSize
    assertTrue(list.buffer.length >= MAX_SIZE || list.buffer.length == 0 && MAX_SIZE == 0);
  }

  @Test
  public void testMaxSizeWithCopyConstructor() {
    MaxSizedFloatArrayList original = new MaxSizedFloatArrayList(MAX_SIZE);
    original.add(1.5f, 2.5f, 3.5f);

    MaxSizedFloatArrayList copy = new MaxSizedFloatArrayList(original);
    assertEquals(original.maxSize, copy.maxSize);
    assertEquals(original.size(), copy.size());
    assertArrayEquals(original.toArray(), copy.toArray(), 0.001f);
  }

  @Test
  public void testHashCodeWithMaxSize() {
    MaxSizedFloatArrayList list1 = new MaxSizedFloatArrayList(5);
    MaxSizedFloatArrayList list2 = new MaxSizedFloatArrayList(10);

    // Same elements but different maxSize should have different hash codes
    list1.add(1.5f, 2.5f, 3.5f);
    list2.add(1.5f, 2.5f, 3.5f);
    assertNotEquals(list1.hashCode(), list2.hashCode());

    // Same maxSize and same elements should have same hash code
    MaxSizedFloatArrayList list3 = new MaxSizedFloatArrayList(5);
    list3.add(1.5f, 2.5f, 3.5f);
    assertEquals(list1.hashCode(), list3.hashCode());
  }

  @Test
  public void testEqualsWithMaxSize() {
    MaxSizedFloatArrayList list1 = new MaxSizedFloatArrayList(5);
    MaxSizedFloatArrayList list2 = new MaxSizedFloatArrayList(10);
    MaxSizedFloatArrayList list3 = new MaxSizedFloatArrayList(5);

    // Same elements but different maxSize should not be equal
    list1.add(1.5f, 2.5f, 3.5f);
    list2.add(1.5f, 2.5f, 3.5f);
    assertNotEquals(list1, list2);

    // Same maxSize and same elements should be equal
    list3.add(1.5f, 2.5f, 3.5f);
    assertEquals(list1, list3);

    // Different elements but same maxSize should not be equal
    list3.clear();
    list3.add(1.5f, 2.5f, 4.5f);
    assertNotEquals(list1, list3);
  }
}
