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

/** Tests for {@link MaxSizedIntArrayList}. */
public class TestMaxSizedIntArrayList extends TestIntArrayList {
  private static final int MAX_SIZE = 5;

  @Test
  public void testMaxSizeConstructor() {
    // Test valid maxSize
    MaxSizedIntArrayList list = new MaxSizedIntArrayList(MAX_SIZE, 3);
    assertEquals(3, list.buffer.length);
    assertEquals(MAX_SIZE, list.maxSize);

    // Test invalid maxSize (expectedElements > maxSize)
    expectThrows(
        AssertionError.class,
        () -> {
          new MaxSizedIntArrayList(2, 3);
        });
  }

  @Test
  public void testMaxSizeLimit() {
    MaxSizedIntArrayList list = new MaxSizedIntArrayList(MAX_SIZE);

    // Fill up to maxSize
    for (int i = 0; i < MAX_SIZE; i++) {
      list.add(i);
    }
    assertEquals(MAX_SIZE, list.size());

    // Try to add beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.add(MAX_SIZE);
        });

    // Try to add array beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.add(new int[] {1, 2, 3});
        });

    // Try to addAll beyond maxSize
    IntArrayList otherList = new IntArrayList();
    otherList.add(1);
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.addAll(otherList);
        });
  }

  @Test
  public void testMaxSizeWithInsert() {
    MaxSizedIntArrayList list = new MaxSizedIntArrayList(MAX_SIZE);

    // Fill up to maxSize - 1
    for (int i = 0; i < MAX_SIZE - 1; i++) {
      list.add(i);
    }
    assertEquals(MAX_SIZE - 1, list.size());

    // Insert at the end (should succeed)
    list.insert(MAX_SIZE - 1, MAX_SIZE - 1);
    assertEquals(MAX_SIZE, list.size());

    // Try to insert beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.insert(0, MAX_SIZE);
        });
  }

  @Test
  public void testMaxSizeWithResize() {
    MaxSizedIntArrayList list = new MaxSizedIntArrayList(MAX_SIZE);

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
    MaxSizedIntArrayList list = new MaxSizedIntArrayList(MAX_SIZE);

    // Try to ensure capacity beyond maxSize
    expectThrows(
        IllegalStateException.class,
        () -> {
          list.ensureCapacity(MAX_SIZE + 1);
        });

    // Ensure capacity to maxSize (should succeed)
    list.ensureCapacity(MAX_SIZE);
    assertEquals(MAX_SIZE, list.buffer.length);
  }

  @Test
  public void testMaxSizeWithCopyConstructor() {
    MaxSizedIntArrayList original = new MaxSizedIntArrayList(MAX_SIZE);
    original.add(1, 2, 3);

    MaxSizedIntArrayList copy = new MaxSizedIntArrayList(original);
    assertEquals(original.maxSize, copy.maxSize);
    assertEquals(original.size(), copy.size());
    assertArrayEquals(original.toArray(), copy.toArray());
  }

  @Test
  public void testHashCodeWithMaxSize() {
    MaxSizedIntArrayList list1 = new MaxSizedIntArrayList(5);
    MaxSizedIntArrayList list2 = new MaxSizedIntArrayList(10);

    // Same elements but different maxSize should have different hash codes
    list1.add(1, 2, 3);
    list2.add(1, 2, 3);
    assertNotEquals(list1.hashCode(), list2.hashCode());

    // Same maxSize and same elements should have same hash code
    MaxSizedIntArrayList list3 = new MaxSizedIntArrayList(5);
    list3.add(1, 2, 3);
    assertEquals(list1.hashCode(), list3.hashCode());
  }

  @Test
  public void testEqualsWithMaxSize() {
    MaxSizedIntArrayList list1 = new MaxSizedIntArrayList(5);
    MaxSizedIntArrayList list2 = new MaxSizedIntArrayList(10);
    MaxSizedIntArrayList list3 = new MaxSizedIntArrayList(5);

    // Same elements but different maxSize should not be equal
    list1.add(1, 2, 3);
    list2.add(1, 2, 3);
    assertNotEquals(list1, list2);

    // Same maxSize and same elements should be equal
    list3.add(1, 2, 3);
    assertEquals(list1, list3);

    // Different elements but same maxSize should not be equal
    list3.clear();
    list3.add(1, 2, 4);
    assertNotEquals(list1, list3);
  }
}
