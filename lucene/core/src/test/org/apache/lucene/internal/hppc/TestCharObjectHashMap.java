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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for {@link CharObjectHashMap}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.CharObjectHashMapTest
 *
 * <p>github: https://github.com/carrotsearch/hppc release: 0.9.0
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestCharObjectHashMap extends LuceneTestCase {
  /* Ready to use key values. */

  private final char keyE = 0;
  private final char key1 = cast(1);
  private final char key2 = cast(2);
  private final char key3 = cast(3);
  private final char key4 = cast(4);

  /** Convert to target type from an integer used to test stuff. */
  private char cast(int v) {
    return (char) ('a' + v);
  }

  /** Create a new array of a given type and copy the arguments to this array. */
  private char[] newArray(char... elements) {
    return elements;
  }

  private static int randomIntBetween(int min, int max) {
    return min + random().nextInt(max + 1 - min);
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(char[] array, char... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    Arrays.sort(elements);
    assertArrayEquals(elements, array);
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(Object[] array, Object... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    assertArrayEquals(elements, array);
  }

  private final int value0 = vcast(0);
  private final int value1 = vcast(1);
  private final int value2 = vcast(2);
  private final int value3 = vcast(3);
  private final int value4 = vcast(4);

  /** Per-test fresh initialized instance. */
  private CharObjectHashMap<Object> map = newInstance();

  private CharObjectHashMap newInstance() {
    return new CharObjectHashMap();
  }

  @After
  public void checkEmptySlotsUninitialized() {
    if (map != null) {
      int occupied = 0;
      for (int i = 0; i <= map.mask; i++) {
        if (((map.keys[i]) == 0)) {

        } else {
          occupied++;
        }
      }
      assertEquals(occupied, map.assigned);

      if (!map.hasEmptyKey) {}
    }
  }

  /** Convert to target type from an integer used to test stuff. */
  private int vcast(int value) {
    return value;
  }

  /** Create a new array of a given type and copy the arguments to this array. */
  /*  */
  private Object[] newvArray(Object... elements) {
    return elements;
  }

  private void assertSameMap(
      final CharObjectHashMap<Object> c1, final CharObjectHashMap<Object> c2) {
    assertEquals(c1.size(), c2.size());

    for (CharObjectHashMap.CharObjectCursor entry : c1) {
      assertTrue(c2.containsKey(entry.key));
      assertEquals(entry.value, c2.get(entry.key));
    }
  }

  /* */
  @Test
  public void testEnsureCapacity() {
    final AtomicInteger expands = new AtomicInteger();
    CharObjectHashMap map =
        new CharObjectHashMap(0) {
          @Override
          protected void allocateBuffers(int arraySize) {
            super.allocateBuffers(arraySize);
            expands.incrementAndGet();
          }
        };

    // Add some elements.
    final int max = rarely() ? 0 : randomIntBetween(0, 250);
    for (int i = 0; i < max; i++) {
      map.put(cast(i), value0);
    }

    final int additions = randomIntBetween(max, max + 5000);
    map.ensureCapacity(additions + map.size());
    final int before = expands.get();
    for (int i = 0; i < additions; i++) {
      map.put(cast(i), value0);
    }
    assertEquals(before, expands.get());
  }

  @Test
  public void testCursorIndexIsValid() {
    map.put(keyE, value1);
    map.put(key1, value2);
    map.put(key2, value3);

    for (CharObjectHashMap.CharObjectCursor c : map) {
      assertTrue(map.indexExists(c.index));
      assertEquals(c.value, map.indexGet(c.index));
    }
  }

  @Test
  public void testIndexMethods() {
    map.put(keyE, value1);
    map.put(key1, value2);

    assertTrue(map.indexOf(keyE) >= 0);
    assertTrue(map.indexOf(key1) >= 0);
    assertTrue(map.indexOf(key2) < 0);

    assertTrue(map.indexExists(map.indexOf(keyE)));
    assertTrue(map.indexExists(map.indexOf(key1)));
    assertFalse(map.indexExists(map.indexOf(key2)));

    assertEquals(value1, map.indexGet(map.indexOf(keyE)));
    assertEquals(value2, map.indexGet(map.indexOf(key1)));

    expectThrows(
        AssertionError.class,
        () -> {
          map.indexGet(map.indexOf(key2));
        });

    assertEquals(value1, map.indexReplace(map.indexOf(keyE), value3));
    assertEquals(value2, map.indexReplace(map.indexOf(key1), value4));
    assertEquals(value3, map.indexGet(map.indexOf(keyE)));
    assertEquals(value4, map.indexGet(map.indexOf(key1)));

    map.indexInsert(map.indexOf(key2), key2, value1);
    assertEquals(value1, map.indexGet(map.indexOf(key2)));
    assertEquals(3, map.size());

    assertEquals(value3, map.indexRemove(map.indexOf(keyE)));
    assertEquals(2, map.size());
    assertEquals(value1, map.indexRemove(map.indexOf(key2)));
    assertEquals(1, map.size());
    assertTrue(map.indexOf(keyE) < 0);
    assertTrue(map.indexOf(key1) >= 0);
    assertTrue(map.indexOf(key2) < 0);
  }

  /* */
  @Test
  public void testCloningConstructor() {
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);

    assertSameMap(map, new CharObjectHashMap(map));
  }

  /* */
  @Test
  public void testFromArrays() {
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);

    CharObjectHashMap map2 =
        CharObjectHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

    assertSameMap(map, map2);
  }

  @Test
  public void testGetOrDefault() {
    map.put(key2, value2);
    assertTrue(map.containsKey(key2));

    map.put(key1, value1);
    assertEquals(value1, map.getOrDefault(key1, value3));
    assertEquals(value3, map.getOrDefault(key3, value3));
    map.remove(key1);
    assertEquals(value3, map.getOrDefault(key1, value3));
  }

  /* */
  @Test
  public void testPut() {
    map.put(key1, value1);

    assertTrue(map.containsKey(key1));
    assertEquals(value1, map.get(key1));
  }

  /* */
  @Test
  public void testNullValue() {
    map.put(key1, null);

    assertTrue(map.containsKey(key1));
    assertNull(map.get(key1));
  }

  @Test
  public void testPutOverExistingKey() {
    map.put(key1, value1);
    assertEquals(value1, map.put(key1, value3));
    assertEquals(value3, map.get(key1));

    assertEquals(value3, map.put(key1, null));
    assertTrue(map.containsKey(key1));
    assertNull(map.get(key1));

    assertNull(map.put(key1, value1));
    assertEquals(value1, map.get(key1));
  }

  /* */
  @Test
  public void testPutWithExpansions() {
    final int COUNT = 10000;
    final Random rnd = new Random(random().nextLong());
    final HashSet<Object> values = new HashSet<Object>();

    for (int i = 0; i < COUNT; i++) {
      final int v = rnd.nextInt();
      final boolean hadKey = values.contains(cast(v));
      values.add(cast(v));

      assertEquals(hadKey, map.containsKey(cast(v)));
      map.put(cast(v), vcast(v));
      assertEquals(values.size(), map.size());
    }
    assertEquals(values.size(), map.size());
  }

  /* */
  @Test
  public void testPutAll() {
    map.put(key1, value1);
    map.put(key2, value1);

    CharObjectHashMap map2 = newInstance();

    map2.put(key2, value2);
    map2.put(keyE, value1);

    // One new key (keyE).
    assertEquals(1, map.putAll(map2));

    // Assert the value under key2 has been replaced.
    assertEquals(value2, map.get(key2));

    // And key3 has been added.
    assertEquals(value1, map.get(keyE));
    assertEquals(3, map.size());
  }

  /* */
  @Test
  public void testPutIfAbsent() {
    assertTrue(map.putIfAbsent(key1, value1));
    assertFalse(map.putIfAbsent(key1, value2));
    assertEquals(value1, map.get(key1));
  }

  /* */
  @Test
  public void testRemove() {
    map.put(key1, value1);
    assertEquals(value1, map.remove(key1));
    assertEquals(null, map.remove(key1));
    assertEquals(0, map.size());

    // These are internals, but perhaps worth asserting too.
    assertEquals(0, map.assigned);
  }

  /* */
  @Test
  public void testEmptyKey() {
    final char empty = 0;

    map.put(empty, value1);
    assertEquals(1, map.size());
    assertEquals(false, map.isEmpty());
    assertEquals(value1, map.get(empty));
    assertEquals(value1, map.getOrDefault(empty, value2));
    assertEquals(true, map.iterator().hasNext());
    assertEquals(empty, map.iterator().next().key);
    assertEquals(value1, map.iterator().next().value);

    map.remove(empty);
    assertEquals(null, map.get(empty));
    assertEquals(0, map.size());

    map.put(empty, null);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(empty));
    assertNull(map.get(empty));

    map.remove(empty);
    assertEquals(0, map.size());
    assertFalse(map.containsKey(empty));
    assertNull(map.get(empty));

    assertEquals(null, map.put(empty, value1));
    assertEquals(value1, map.put(empty, value2));
    map.clear();
    assertFalse(map.indexExists(map.indexOf(empty)));
    assertEquals(null, map.put(empty, value1));
    map.clear();
    assertEquals(null, map.remove(empty));
  }

  /* */
  @Test
  public void testMapKeySet() {
    map.put(key1, value3);
    map.put(key2, value2);
    map.put(key3, value1);

    assertSortedListEquals(map.keys().toArray(), key1, key2, key3);
  }

  /* */
  @Test
  public void testMapKeySetIterator() {
    map.put(key1, value3);
    map.put(key2, value2);
    map.put(key3, value1);

    int counted = 0;
    for (CharCursor c : map.keys()) {
      assertEquals(map.keys[c.index], c.value);
      counted++;
    }
    assertEquals(counted, map.size());
  }

  /* */
  @Test
  public void testClear() {
    map.put(key1, value1);
    map.put(key2, value1);
    map.clear();
    assertEquals(0, map.size());

    // These are internals, but perhaps worth asserting too.
    assertEquals(0, map.assigned);

    // Check values are cleared.
    assertEquals(null, map.put(key1, value1));
    assertEquals(null, map.remove(key2));
    map.clear();

    // Check if the map behaves properly upon subsequent use.
    testPutWithExpansions();
  }

  /* */
  @Test
  public void testRelease() {
    map.put(key1, value1);
    map.put(key2, value1);
    map.release();
    assertEquals(0, map.size());

    // These are internals, but perhaps worth asserting too.
    assertEquals(0, map.assigned);

    // Check if the map behaves properly upon subsequent use.
    testPutWithExpansions();
  }

  /* */
  @Test
  public void testIterable() {
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);
    map.remove(key2);

    int count = 0;
    for (CharObjectHashMap.CharObjectCursor cursor : map) {
      count++;
      assertTrue(map.containsKey(cursor.key));
      assertEquals(cursor.value, map.get(cursor.key));

      assertEquals(cursor.value, map.values[cursor.index]);
      assertEquals(cursor.key, map.keys[cursor.index]);
    }
    assertEquals(count, map.size());

    map.clear();
    assertFalse(map.iterator().hasNext());
  }

  /* */
  @Test
  public void testBug_HPPC73_FullCapacityGet() {
    final AtomicInteger reallocations = new AtomicInteger();
    final int elements = 0x7F;
    map =
        new CharObjectHashMap(elements, 1f) {
          @Override
          protected double verifyLoadFactor(double loadFactor) {
            // Skip load factor sanity range checking.
            return loadFactor;
          }

          @Override
          protected void allocateBuffers(int arraySize) {
            super.allocateBuffers(arraySize);
            reallocations.incrementAndGet();
          }
        };

    int reallocationsBefore = reallocations.get();
    assertEquals(reallocationsBefore, 1);
    for (int i = 1; i <= elements; i++) {
      map.put(cast(i), value1);
    }

    // Non-existent key.
    char outOfSet = cast(elements + 1);
    map.remove(outOfSet);
    assertFalse(map.containsKey(outOfSet));
    assertEquals(reallocationsBefore, reallocations.get());

    // Should not expand because we're replacing an existing element.
    map.put(key1, value2);
    assertEquals(reallocationsBefore, reallocations.get());

    // Remove from a full map.
    map.remove(key1);
    assertEquals(reallocationsBefore, reallocations.get());
    map.put(key1, value2);

    // Check expand on "last slot of a full map" condition.
    map.put(outOfSet, value1);
    assertEquals(reallocationsBefore + 1, reallocations.get());
  }

  @Test
  public void testHashCodeEquals() {
    CharObjectHashMap l0 = newInstance();
    assertEquals(0, l0.hashCode());
    assertEquals(l0, newInstance());

    CharObjectHashMap l1 =
        CharObjectHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

    CharObjectHashMap l2 =
        CharObjectHashMap.from(newArray(key2, key1, key3), newvArray(value2, value1, value3));

    CharObjectHashMap l3 = CharObjectHashMap.from(newArray(key1, key2), newvArray(value2, value1));

    assertEquals(l1.hashCode(), l2.hashCode());
    assertEquals(l1, l2);

    assertFalse(l1.equals(l3));
    assertFalse(l2.equals(l3));
  }

  @Test
  public void testBug_HPPC37() {
    CharObjectHashMap l1 = CharObjectHashMap.from(newArray(key1), newvArray(value1));

    CharObjectHashMap l2 = CharObjectHashMap.from(newArray(key2), newvArray(value1));

    assertFalse(l1.equals(l2));
    assertFalse(l2.equals(l1));
  }

  /** Runs random insertions/deletions/clearing and compares the results against {@link HashMap}. */
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testAgainstHashMap() {
    final Random rnd = RandomizedTest.getRandom();
    final HashMap other = new HashMap();

    for (int size = 1000; size < 20000; size += 4000) {
      other.clear();
      map.clear();

      for (int round = 0; round < size * 20; round++) {
        char key = cast(rnd.nextInt(size));
        if (rnd.nextInt(50) == 0) {
          key = 0;
        }

        int value = vcast(rnd.nextInt());

        if (rnd.nextBoolean()) {
          Object previousValue;
          if (rnd.nextBoolean()) {
            int index = map.indexOf(key);
            if (map.indexExists(index)) {
              previousValue = map.indexReplace(index, value);
            } else {
              map.indexInsert(index, key, value);
              previousValue = null;
            }
          } else {
            previousValue = map.put(key, value);
          }
          assertEquals(other.put(key, value), previousValue);

          assertEquals(value, map.get(key));
          assertEquals(value, map.indexGet(map.indexOf(key)));
          assertTrue(map.containsKey(key));
          assertTrue(map.indexExists(map.indexOf(key)));
        } else {
          assertEquals(other.containsKey(key), map.containsKey(key));
          Object previousValue =
              map.containsKey(key) && rnd.nextBoolean()
                  ? map.indexRemove(map.indexOf(key))
                  : map.remove(key);
          assertEquals(other.remove(key), previousValue);
        }

        assertEquals(other.size(), map.size());
      }
    }
  }

  /*
   *
   */
  @Test
  public void testClone() {
    this.map.put(key1, value1);
    this.map.put(key2, value2);
    this.map.put(key3, value3);

    CharObjectHashMap cloned = map.clone();
    cloned.remove(key1);

    assertSortedListEquals(map.keys().toArray(), key1, key2, key3);
    assertSortedListEquals(cloned.keys().toArray(), key2, key3);
  }

  /* */
  @Test
  public void testMapValues() {
    map.put(key1, value3);
    map.put(key2, value2);
    map.put(key3, value1);
    assertSortedListEquals(map.values().toArray(), value1, value2, value3);

    map.clear();
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value2);
    assertSortedListEquals(map.values().toArray(), value1, value2, value2);
  }

  /* */
  @Test
  public void testMapValuesIterator() {
    map.put(key1, value3);
    map.put(key2, value2);
    map.put(key3, value1);

    int counted = 0;
    for (ObjectCursor c : map.values()) {
      assertEquals(map.values[c.index], c.value);
      counted++;
    }
    assertEquals(counted, map.size());
  }

  /* */
  @Test
  public void testEqualsSameClass() {
    CharObjectHashMap l1 = newInstance();
    l1.put(key1, value0);
    l1.put(key2, value1);
    l1.put(key3, value2);

    CharObjectHashMap l2 = new CharObjectHashMap(l1);
    l2.putAll(l1);

    CharObjectHashMap l3 = new CharObjectHashMap(l2);
    l3.putAll(l2);
    l3.put(key4, value0);

    assertEquals(l2, l1);
    assertEquals(l2.hashCode(), l1.hashCode());
    assertNotEquals(l1, l3);
  }

  /* */
  @Test
  public void testEqualsSubClass() {
    class Sub extends CharObjectHashMap {}

    CharObjectHashMap l1 = newInstance();
    l1.put(key1, value0);
    l1.put(key2, value1);
    l1.put(key3, value2);

    CharObjectHashMap l2 = new Sub();
    l2.putAll(l1);
    l2.put(key4, value3);

    CharObjectHashMap l3 = new Sub();
    l3.putAll(l2);

    assertNotEquals(l1, l2);
    assertEquals(l3.hashCode(), l2.hashCode());
    assertEquals(l3, l2);
  }
}
