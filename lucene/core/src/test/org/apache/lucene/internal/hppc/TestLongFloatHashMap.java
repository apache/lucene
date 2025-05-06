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
import org.junit.Test;

/**
 * Tests for {@link LongFloatHashMap}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongFloatHashMapTest
 *
 * <p>github: https://github.com/carrotsearch/hppc release: 0.9.0
 */
public class TestLongFloatHashMap extends LuceneTestCase {
  /* Ready to use key values. */

  private final long keyE = 0;
  private final long key1 = cast(1);
  private final long key2 = cast(2);
  private final long key3 = cast(3);
  private final long key4 = cast(4);

  /** Convert to target type from an integer used to test stuff. */
  private long cast(int v) {
    return v;
  }

  /** Create a new array of a given type and copy the arguments to this array. */
  private long[] newArray(long... elements) {
    return elements;
  }

  private static int randomIntBetween(int min, int max) {
    return min + random().nextInt(max + 1 - min);
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(long[] array, long... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    Arrays.sort(elements);
    assertArrayEquals(elements, array);
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(float[] array, float... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    Arrays.sort(elements);
    assertArrayEquals(elements, array);
  }

  private final int value0 = vcast(0);
  private final int value1 = vcast(1);
  private final int value2 = vcast(2);
  private final int value3 = vcast(3);
  private final int value4 = vcast(4);

  /** Per-test fresh initialized instance. */
  private LongFloatHashMap map = newInstance();

  private LongFloatHashMap newInstance() {
    return new LongFloatHashMap();
  }

  /** Convert to target type from an integer used to test stuff. */
  private int vcast(int value) {
    return value;
  }

  /** Create a new array of a given type and copy the arguments to this array. */
  /*  */
  private float[] newvArray(int... elements) {
    float[] v = new float[elements.length];
    for (int i = 0; i < elements.length; i++) {
      v[i] = elements[i];
    }
    return v;
  }

  private void assertSameMap(final LongFloatHashMap c1, final LongFloatHashMap c2) {
    assertEquals(c1.size(), c2.size());

    for (LongFloatHashMap.LongFloatCursor entry : c1) {
      assertTrue(c2.containsKey(entry.key));
      assertEquals2(entry.value, c2.get(entry.key));
    }
  }

  private static void assertEquals2(float v1, float v2) {
    assertEquals(v1, v2, 0f);
  }

  private static void assertArrayEquals(float[] v1, float[] v2) {
    assertArrayEquals(v1, v2, 0f);
  }

  /* */
  @Test
  public void testEnsureCapacity() {
    final AtomicInteger expands = new AtomicInteger();
    LongFloatHashMap map =
        new LongFloatHashMap(0) {
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

    for (LongFloatHashMap.LongFloatCursor c : map) {
      assertTrue(map.indexExists(c.index));
      assertEquals2(c.value, map.indexGet(c.index));
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

    assertEquals2(value1, map.indexGet(map.indexOf(keyE)));
    assertEquals2(value2, map.indexGet(map.indexOf(key1)));

    expectThrows(
        AssertionError.class,
        () -> {
          map.indexGet(map.indexOf(key2));
        });

    assertEquals2(value1, map.indexReplace(map.indexOf(keyE), value3));
    assertEquals2(value2, map.indexReplace(map.indexOf(key1), value4));
    assertEquals2(value3, map.indexGet(map.indexOf(keyE)));
    assertEquals2(value4, map.indexGet(map.indexOf(key1)));

    map.indexInsert(map.indexOf(key2), key2, value1);
    assertEquals2(value1, map.indexGet(map.indexOf(key2)));
    assertEquals(3, map.size());

    assertEquals2(value3, map.indexRemove(map.indexOf(keyE)));
    assertEquals(2, map.size());
    assertEquals2(value1, map.indexRemove(map.indexOf(key2)));
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

    assertSameMap(map, new LongFloatHashMap(map));
  }

  /* */
  @Test
  public void testFromArrays() {
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);

    LongFloatHashMap map2 =
        LongFloatHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

    assertSameMap(map, map2);
  }

  @Test
  public void testGetOrDefault() {
    map.put(key2, value2);
    assertTrue(map.containsKey(key2));

    map.put(key1, value1);
    assertEquals2(value1, map.getOrDefault(key1, value3));
    assertEquals2(value3, map.getOrDefault(key3, value3));
    map.remove(key1);
    assertEquals2(value3, map.getOrDefault(key1, value3));
  }

  /* */
  @Test
  public void testPut() {
    map.put(key1, value1);

    assertTrue(map.containsKey(key1));
    assertEquals2(value1, map.get(key1));
  }

  /* */
  @Test
  public void testPutOverExistingKey() {
    map.put(key1, value1);
    assertEquals2(value1, map.put(key1, value3));
    assertEquals2(value3, map.get(key1));
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

    LongFloatHashMap map2 = newInstance();

    map2.put(key2, value2);
    map2.put(keyE, value1);

    // One new key (keyE).
    assertEquals(1, map.putAll(map2));

    // Assert the value under key2 has been replaced.
    assertEquals2(value2, map.get(key2));

    // And key3 has been added.
    assertEquals2(value1, map.get(keyE));
    assertEquals(3, map.size());
  }

  /* */
  @Test
  public void testPutIfAbsent() {
    assertTrue(map.putIfAbsent(key1, value1));
    assertFalse(map.putIfAbsent(key1, value2));
    assertEquals2(value1, map.get(key1));
  }

  @Test
  public void testPutOrAdd() {
    assertEquals2(value1, map.putOrAdd(key1, value1, value2));
    assertEquals2(value3, map.putOrAdd(key1, value1, value2));
  }

  @Test
  public void testAddTo() {
    assertEquals2(value1, map.addTo(key1, value1));
    assertEquals2(value3, map.addTo(key1, value2));
  }

  /* */
  @Test
  public void testRemove() {
    map.put(key1, value1);
    assertEquals2(value1, map.remove(key1));
    assertEquals2(0, map.remove(key1));
    assertEquals(0, map.size());

    // These are internals, but perhaps worth asserting too.
    assertEquals(0, map.assigned);
  }

  /* */
  @Test
  public void testEmptyKey() {
    final int empty = 0;

    map.put(empty, value1);
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());
    assertEquals2(value1, map.get(empty));
    assertEquals2(value1, map.getOrDefault(empty, value2));
    assertTrue(map.iterator().hasNext());
    assertEquals(empty, map.iterator().next().key);
    assertEquals2(value1, map.iterator().next().value);

    map.remove(empty);
    assertEquals2(0, map.get(empty));
    assertEquals(0, map.size());

    assertEquals2(0, map.put(empty, value1));
    assertEquals2(value1, map.put(empty, value2));
    map.clear();
    assertFalse(map.indexExists(map.indexOf(empty)));
    assertEquals2(0, map.put(empty, value1));
    map.clear();
    assertEquals2(0, map.remove(empty));
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
    for (LongCursor c : map.keys()) {
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
    assertEquals2(0, map.put(key1, value1));
    assertEquals2(0, map.remove(key2));
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
    for (LongFloatHashMap.LongFloatCursor cursor : map) {
      count++;
      assertTrue(map.containsKey(cursor.key));
      assertEquals2(cursor.value, map.get(cursor.key));

      assertEquals2(cursor.value, map.values[cursor.index]);
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
        new LongFloatHashMap(elements, 1f) {
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
    long outOfSet = cast(elements + 1);
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
    LongFloatHashMap l0 = newInstance();
    assertEquals(0, l0.hashCode());
    assertEquals(l0, newInstance());

    LongFloatHashMap l1 =
        LongFloatHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

    LongFloatHashMap l2 =
        LongFloatHashMap.from(newArray(key2, key1, key3), newvArray(value2, value1, value3));

    LongFloatHashMap l3 = LongFloatHashMap.from(newArray(key1, key2), newvArray(value2, value1));

    assertEquals(l1.hashCode(), l2.hashCode());
    assertEquals(l1, l2);

    assertNotEquals(l1, l3);
    assertNotEquals(l2, l3);
  }

  @Test
  public void testBug_HPPC37() {
    LongFloatHashMap l1 = LongFloatHashMap.from(newArray(key1), newvArray(value1));

    LongFloatHashMap l2 = LongFloatHashMap.from(newArray(key2), newvArray(value1));

    assertNotEquals(l1, l2);
    assertNotEquals(l2, l1);
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
        long key = cast(rnd.nextInt(size));
        if (rnd.nextInt(50) == 0) {
          key = 0;
        }

        float value = vcast(rnd.nextInt());

        boolean hadOldValue = map.containsKey(key);
        if (rnd.nextBoolean()) {
          float previousValue;
          if (rnd.nextBoolean()) {
            int index = map.indexOf(key);
            if (map.indexExists(index)) {
              previousValue = map.indexReplace(index, value);
            } else {
              map.indexInsert(index, key, value);
              previousValue = 0;
            }
          } else {
            previousValue = map.put(key, value);
          }
          assertEquals(
              other.put(key, value), ((previousValue) == 0) && !hadOldValue ? null : previousValue);

          assertEquals2(value, map.get(key));
          assertEquals2(value, map.indexGet(map.indexOf(key)));
          assertTrue(map.containsKey(key));
          assertTrue(map.indexExists(map.indexOf(key)));
        } else {
          assertEquals(other.containsKey(key), map.containsKey(key));
          float previousValue =
              map.containsKey(key) && rnd.nextBoolean()
                  ? map.indexRemove(map.indexOf(key))
                  : map.remove(key);
          assertEquals(
              other.remove(key), ((previousValue) == 0) && !hadOldValue ? null : previousValue);
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

    LongFloatHashMap cloned = map.clone();
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
    for (FloatCursor c : map.values()) {
      assertEquals2(map.values[c.index], c.value);
      counted++;
    }
    assertEquals(counted, map.size());
  }

  /* */
  @Test
  public void testEqualsSameClass() {
    LongFloatHashMap l1 = newInstance();
    l1.put(key1, value0);
    l1.put(key2, value1);
    l1.put(key3, value2);

    LongFloatHashMap l2 = new LongFloatHashMap(l1);
    l2.putAll(l1);

    LongFloatHashMap l3 = new LongFloatHashMap(l2);
    l3.putAll(l2);
    l3.put(key4, value0);

    assertEquals(l2, l1);
    assertEquals(l2.hashCode(), l1.hashCode());
    assertNotEquals(l1, l3);
  }

  /* */
  @Test
  public void testEqualsSubClass() {
    class Sub extends LongFloatHashMap {}

    LongFloatHashMap l1 = newInstance();
    l1.put(key1, value0);
    l1.put(key2, value1);
    l1.put(key3, value2);

    LongFloatHashMap l2 = new Sub();
    l2.putAll(l1);
    l2.put(key4, value3);

    LongFloatHashMap l3 = new Sub();
    l3.putAll(l2);

    assertNotEquals(l1, l2);
    assertEquals(l3.hashCode(), l2.hashCode());
    assertEquals(l3, l2);
  }
}
