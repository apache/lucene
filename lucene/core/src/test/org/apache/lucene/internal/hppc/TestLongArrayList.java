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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link LongArrayList}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongArrayListTest
 *
 * <p>github: https://github.com/carrotsearch/hppc release: 0.9.0
 */
public class TestLongArrayList extends LuceneTestCase {
  private final long key0 = cast(0);
  private final long key1 = cast(1);
  private final long key2 = cast(2);
  private final long key3 = cast(3);
  private final long key4 = cast(4);
  private final long key5 = cast(5);
  private final long key6 = cast(6);
  private final long key7 = cast(7);

  /** Convert to target type from an integer used to test stuff. */
  private long cast(int v) {
    return v;
  }

  /** Per-test fresh initialized instance. */
  private LongArrayList list;

  @Before
  public void initialize() {
    list = new LongArrayList();
  }

  @Test
  public void testInitiallyEmpty() {
    assertEquals(0, list.size());
  }

  @Test
  public void testAdd() {
    list.add(key1, key2);
    assertListEquals(list.toArray(), 1, 2);
  }

  @Test
  public void testAddTwoArgs() {
    list.add(key1, key2);
    list.add(key3, key4);
    assertListEquals(list.toArray(), 1, 2, 3, 4);
  }

  @Test
  public void testAddArray() {
    list.add(asArray(0, 1, 2, 3), 1, 2);
    assertListEquals(list.toArray(), 1, 2);
  }

  @Test
  public void testAddVarArg() {
    list.add(asArray(0, 1, 2, 3));
    list.add(key4, key5, key6, key7);
    assertListEquals(list.toArray(), 0, 1, 2, 3, 4, 5, 6, 7);
  }

  @Test
  public void testAddAll() {
    LongArrayList list2 = new LongArrayList();
    list2.add(asArray(0, 1, 2));

    list.addAll(list2);
    list.addAll(list2);

    assertListEquals(list.toArray(), 0, 1, 2, 0, 1, 2);
  }

  @Test
  public void testInsert() {
    list.insert(0, key1);
    list.insert(0, key2);
    list.insert(2, key3);
    list.insert(1, key4);

    assertListEquals(list.toArray(), 2, 4, 1, 3);
  }

  @Test
  public void testSet() {
    list.add(asArray(0, 1, 2));

    assertEquals(0, list.set(0, key3));
    assertEquals(1, list.set(1, key4));
    assertEquals(2, list.set(2, key5));

    assertListEquals(list.toArray(), 3, 4, 5);
  }

  @Test
  public void testRemoveAt() {
    list.add(asArray(0, 1, 2, 3, 4));

    list.removeAt(0);
    list.removeAt(2);
    list.removeAt(1);

    assertListEquals(list.toArray(), 1, 4);
  }

  @Test
  public void testRemoveLast() {
    list.add(asArray(0, 1, 2, 3, 4));

    assertEquals(4, list.removeLast());
    assertEquals(4, list.size());
    assertListEquals(list.toArray(), 0, 1, 2, 3);
    assertEquals(3, list.removeLast());
    assertEquals(3, list.size());
    assertListEquals(list.toArray(), 0, 1, 2);
    assertEquals(2, list.removeLast());
    assertEquals(1, list.removeLast());
    assertEquals(0, list.removeLast());
    assertTrue(list.isEmpty());
  }

  @Test
  public void testRemoveElement() {
    list.add(asArray(0, 1, 2, 3, 3, 4));

    assertTrue(list.removeElement(3));
    assertTrue(list.removeElement(2));
    assertFalse(list.removeElement(5));

    assertListEquals(list.toArray(), 0, 1, 3, 4);
  }

  @Test
  public void testRemoveRange() {
    list.add(asArray(0, 1, 2, 3, 4));

    list.removeRange(0, 2);
    assertListEquals(list.toArray(), 2, 3, 4);

    list.removeRange(2, 3);
    assertListEquals(list.toArray(), 2, 3);

    list.removeRange(1, 1);
    assertListEquals(list.toArray(), 2, 3);

    list.removeRange(0, 1);
    assertListEquals(list.toArray(), 3);
  }

  @Test
  public void testRemoveFirstLast() {
    list.add(asArray(0, 1, 2, 1, 0));

    assertEquals(-1, list.removeFirst(key5));
    assertEquals(-1, list.removeLast(key5));
    assertListEquals(list.toArray(), 0, 1, 2, 1, 0);

    assertEquals(1, list.removeFirst(key1));
    assertListEquals(list.toArray(), 0, 2, 1, 0);
    assertEquals(3, list.removeLast(key0));
    assertListEquals(list.toArray(), 0, 2, 1);
    assertEquals(0, list.removeLast(key0));
    assertListEquals(list.toArray(), 2, 1);
    assertEquals(-1, list.removeLast(key0));
  }

  @Test
  public void testRemoveAll() {
    list.add(asArray(0, 1, 0, 1, 0));

    assertEquals(0, list.removeAll(key2));
    assertEquals(3, list.removeAll(key0));
    assertListEquals(list.toArray(), 1, 1);

    assertEquals(2, list.removeAll(key1));
    assertTrue(list.isEmpty());
  }

  @Test
  public void testIndexOf() {
    list.add(asArray(0, 1, 2, 1, 0));

    assertEquals(0, list.indexOf(key0));
    assertEquals(-1, list.indexOf(key3));
    assertEquals(2, list.indexOf(key2));
  }

  @Test
  public void testLastIndexOf() {
    list.add(asArray(0, 1, 2, 1, 0));

    assertEquals(4, list.lastIndexOf(key0));
    assertEquals(-1, list.lastIndexOf(key3));
    assertEquals(2, list.lastIndexOf(key2));
  }

  @Test
  public void testEnsureCapacity() {
    LongArrayList list = new LongArrayList(0);
    assertEquals(list.size(), list.buffer.length);
    long[] buffer1 = list.buffer;
    list.ensureCapacity(100);
    assertNotSame(buffer1, list.buffer);
  }

  @Test
  public void testResizeAndCleanBuffer() {
    list.ensureCapacity(20);
    Arrays.fill(list.buffer, key1);

    list.resize(10);
    assertEquals(10, list.size());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(0, list.get(i));
    }

    Arrays.fill(list.buffer, 0);
    for (int i = 5; i < list.size(); i++) {
      list.set(i, key1);
    }
    list.resize(5);
    assertEquals(5, list.size());
    for (int i = list.size(); i < list.buffer.length; i++) {
      assertEquals(0, list.buffer[i]);
    }
  }

  @Test
  public void testTrimToSize() {
    list.add(asArray(1, 2));
    list.trimToSize();
    assertEquals(2, list.buffer.length);
  }

  @Test
  public void testRelease() {
    list.add(asArray(1, 2));
    list.release();
    assertEquals(0, list.size());
    list.add(asArray(1, 2));
    assertEquals(2, list.size());
  }

  @Test
  public void testIterable() {
    list.add(asArray(0, 1, 2, 3));
    int count = 0;
    for (LongCursor cursor : list) {
      count++;
      assertEquals(list.get(cursor.index), cursor.value);
      assertEquals(list.buffer[cursor.index], cursor.value);
    }
    assertEquals(count, list.size());

    count = 0;
    list.resize(0);
    for (@SuppressWarnings("unused") LongCursor cursor : list) {
      count++;
    }
    assertEquals(0, count);
  }

  @Test
  public void testIterator() {
    list.add(asArray(0, 1, 2, 3));
    Iterator<LongCursor> iterator = list.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      iterator.hasNext();
      iterator.hasNext();
      iterator.hasNext();
      iterator.next();
      count++;
    }
    assertEquals(count, list.size());

    list.resize(0);
    assertFalse(list.iterator().hasNext());
  }

  @Test
  public void testClear() {
    list.add(asArray(1, 2, 3));
    list.clear();
    assertTrue(list.isEmpty());
    assertEquals(-1, list.indexOf(cast(1)));
  }

  @Test
  public void testFrom() {
    list = LongArrayList.from(key1, key2, key3);
    assertEquals(3, list.size());
    assertListEquals(list.toArray(), 1, 2, 3);
    assertEquals(list.size(), list.buffer.length);
  }

  @Test
  public void testCopyList() {
    list.add(asArray(1, 2, 3));
    LongArrayList copy = new LongArrayList(list);
    assertEquals(3, copy.size());
    assertListEquals(copy.toArray(), 1, 2, 3);
    assertEquals(copy.size(), copy.buffer.length);
  }

  @Test
  public void testHashCodeEquals() {
    LongArrayList l0 = LongArrayList.from();
    assertEquals(1, l0.hashCode());
    assertEquals(l0, LongArrayList.from());

    LongArrayList l1 = LongArrayList.from(key1, key2, key3);
    LongArrayList l2 = LongArrayList.from(key1, key2);
    l2.add(key3);

    assertEquals(l1.hashCode(), l2.hashCode());
    assertEquals(l1, l2);
  }

  @Test
  public void testEqualElements() {
    LongArrayList l1 = LongArrayList.from(key1, key2, key3);
    LongArrayList l2 = LongArrayList.from(key1, key2);
    l2.add(key3);

    assertEquals(l1.hashCode(), l2.hashCode());
    assertTrue(l2.equalElements(l1));
  }

  @Test
  public void testToArray() {
    LongArrayList l1 = LongArrayList.from(key1, key2, key3);
    l1.ensureCapacity(100);
    long[] result = l1.toArray();
    assertArrayEquals(new long[] {key1, key2, key3}, result);
  }

  @Test
  public void testClone() {
    list.add(key1, key2, key3);

    LongArrayList cloned = list.clone();
    cloned.removeAt(cloned.indexOf(key1));

    assertSortedListEquals(list.toArray(), key1, key2, key3);
    assertSortedListEquals(cloned.toArray(), key2, key3);
  }

  @Test
  public void testToString() {
    assertEquals(
        "[" + key1 + ", " + key2 + ", " + key3 + "]",
        LongArrayList.from(key1, key2, key3).toString());
  }

  @Test
  public void testEqualsSameClass() {
    LongArrayList l1 = LongArrayList.from(key1, key2, key3);
    LongArrayList l2 = LongArrayList.from(key1, key2, key3);
    LongArrayList l3 = LongArrayList.from(key1, key3, key2);

    assertEquals(l1, l2);
    assertEquals(l1.hashCode(), l2.hashCode());
    assertNotEquals(l1, l3);
  }

  @Test
  public void testEqualsSubClass() {
    class Sub extends LongArrayList {}
    ;

    LongArrayList l1 = LongArrayList.from(key1, key2, key3);
    LongArrayList l2 = new Sub();
    LongArrayList l3 = new Sub();
    l2.addAll(l1);
    l3.addAll(l1);

    assertEquals(l2, l3);
    assertNotEquals(l1, l3);
  }

  @Test
  public void testStream() {
    assertEquals(key1, LongArrayList.from(key1, key2, key3).stream().min().orElseThrow());
    assertEquals(key3, LongArrayList.from(key2, key1, key3).stream().max().orElseThrow());
    assertEquals(0, LongArrayList.from(key1, key2, -key3).stream().sum());
    expectThrows(
        NoSuchElementException.class,
        () -> {
          LongArrayList.from().stream().min().orElseThrow();
        });
  }

  @Test
  public void testSort() {
    list.add(key3, key1, key3, key2);
    LongArrayList list2 = new LongArrayList();
    list2.ensureCapacity(100);
    list2.addAll(list);
    assertSame(list2, list2.sort());
    assertEquals(LongArrayList.from(key1, key2, key3, key3), list2);
  }

  @Test
  public void testReverse() {
    for (int i = 0; i < 10; i++) {
      long[] elements = new long[i];
      for (int j = 0; j < i; j++) {
        elements[j] = cast(j);
      }
      LongArrayList list = new LongArrayList();
      list.ensureCapacity(30);
      list.add(elements);
      assertSame(list, list.reverse());
      assertEquals(elements.length, list.size());
      int reverseIndex = elements.length - 1;
      for (LongCursor cursor : list) {
        assertEquals(elements[reverseIndex--], cursor.value);
      }
    }
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertListEquals(long[] array, long... elements) {
    assertEquals(elements.length, array.length);
    assertArrayEquals(elements, array);
  }

  private static long[] asArray(long... elements) {
    return elements;
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(long[] array, long... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    Arrays.sort(elements);
    assertArrayEquals(elements, array);
  }
}
