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
 * Tests for {@link IntLongHashMap}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.IntLongHashMapTest
 *
 * <p>github: https://github.com/carrotsearch/hppc release: 0.10.0
 */
public class TestIntLongHashMap extends LuceneTestCase {

  /* Ready to use key values. */

  protected int keyE = 0;
  protected int key0 = cast(0), k0 = key0;
  protected int key1 = cast(1), k1 = key1;
  protected int key2 = cast(2), k2 = key2;
  protected int key3 = cast(3), k3 = key3;
  protected int key4 = cast(4), k4 = key4;
  protected int key5 = cast(5), k5 = key5;
  protected int key6 = cast(6), k6 = key6;
  protected int key7 = cast(7), k7 = key7;
  protected int key8 = cast(8), k8 = key8;
  protected int key9 = cast(9), k9 = key9;

  protected long value0 = vcast(0);
  protected long value1 = vcast(1);
  protected long value2 = vcast(2);
  protected long value3 = vcast(3);
  protected long value4 = vcast(4);

  private static int randomIntBetween(int min, int max) {
    return min + random().nextInt(max + 1 - min);
  }

  private final int[] newArray(int... elements) {
    return elements;
  }

  /** Create a new array of a given type and copy the arguments to this array. */
  /*  */
  private final long[] newvArray(long... elements) {
    return elements;
  }

  /** Convert to target type from an integer used to test stuff. */
  private int cast(Integer v) {
    return v.intValue();
  }

  /** Convert to target type from an integer used to test stuff. */
  private long vcast(int value) {
    return (long) value;
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  public static void assertSortedListEquals(int[] array, int... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    Arrays.sort(elements);
    assertArrayEquals(elements, array);
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  public static void assertSortedListEquals(long[] array, long... elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    assertArrayEquals(elements, array);
  }

  /** Per-test fresh initialized instance. */
  public IntLongHashMap map = newInstance();

  protected IntLongHashMap newInstance() {
    return new IntLongHashMap();
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

  private void assertSameMap(final IntLongHashMap c1, final IntLongHashMap c2) {
    assertEquals(c1.size(), c2.size());

    for (IntLongHashMap.IntLongCursor entry : c1) {
      assertTrue(c2.containsKey(entry.key));
      assertEquals(entry.value, c2.get(entry.key));
    }
  }

  /* */
  @Test
  public void testEnsureCapacity() {
    final AtomicInteger expands = new AtomicInteger();
    IntLongHashMap map =
        new IntLongHashMap(0) {
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

    for (IntLongHashMap.IntLongCursor c : map) {
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

    assertSameMap(map, new IntLongHashMap(map));
  }

  /* */
  @Test
  public void testFromArrays() {
    map.put(key1, value1);
    map.put(key2, value2);
    map.put(key3, value3);

    IntLongHashMap map2 =
        IntLongHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

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

    map.put(key2, 0L);

    assertEquals(2, map.size());
    assertTrue(map.containsKey(key2));
    assertEquals(0L, map.get(key2));
  }

  /* */
  @Test
  public void testPutOverExistingKey() {
    map.put(key1, value1);
    assertEquals(value1, map.put(key1, value3));
    assertEquals(value3, map.get(key1));
    assertEquals(1, map.size());

    assertEquals(value3, map.put(key1, 0L));
    assertTrue(map.containsKey(key1));
    assertEquals(0L, map.get(key1));

    assertEquals(0L, map.put(key1, value1));
    assertEquals(value1, map.get(key1));
    assertEquals(1, map.size());
  }

  /* */
  @Test
  public void testPutWithExpansions() {
    final int COUNT = 10000;
    final Random rnd = new Random(random().nextLong());
    final HashSet<Object> values = new HashSet<>();

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

    IntLongHashMap map2 = newInstance();

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

  @Test
  public void testPutOrAdd() {
    assertEquals(value1, map.putOrAdd(key1, value1, value2));
    assertEquals(value3, map.putOrAdd(key1, value1, value2));
  }

  @Test
  public void testAddTo() {
    assertEquals(value1, map.addTo(key1, value1));
    assertEquals(value3, map.addTo(key1, value2));
  }

  /* */
  @Test
  public void testRemove() {
    map.put(key1, value1);
    assertEquals(value1, map.remove(key1));
    assertEquals(0L, map.remove(key1));
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
    assertEquals(false, map.isEmpty());
    assertEquals(value1, map.get(empty));
    assertEquals(value1, map.getOrDefault(empty, value2));
    assertEquals(true, map.iterator().hasNext());
    assertEquals(empty, map.iterator().next().key);
    assertEquals(value1, map.iterator().next().value);

    assertEquals(1, map.keys().size());
    assertEquals(empty, map.keys().iterator().next().value);
    assertEquals(value1, map.values().iterator().next().value);

    assertEquals(value1, map.put(empty, 0L));
    assertEquals(1, map.size());
    assertTrue(map.containsKey(empty));
    assertEquals(0L, map.get(empty));

    map.remove(empty);
    assertEquals(0L, map.get(empty));
    assertEquals(0, map.size());

    assertEquals(0L, map.put(empty, value1));
    assertEquals(value1, map.put(empty, value2));
    map.clear();
    assertFalse(map.indexExists(map.indexOf(empty)));
    assertEquals(0L, map.put(empty, value1));
    map.clear();
    assertEquals(0L, map.remove(empty));
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
    for (IntCursor c : map.keys()) {
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
    assertEquals(0L, map.put(key1, value1));
    assertEquals(0L, map.remove(key2));
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
    for (IntLongHashMap.IntLongCursor cursor : map) {
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
        new IntLongHashMap(elements, 1f) {
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
    int outOfSet = cast(elements + 1);
    map.remove(outOfSet);
    assertFalse(map.containsKey(outOfSet));
    assertEquals(reallocationsBefore, reallocations.get());

    // Should not expand because we're replacing an existing element.
    map.put(k1, value2);
    assertEquals(reallocationsBefore, reallocations.get());

    // Remove from a full map.
    map.remove(k1);
    assertEquals(reallocationsBefore, reallocations.get());
    map.put(k1, value2);

    // Check expand on "last slot of a full map" condition.
    map.put(outOfSet, value1);
    assertEquals(reallocationsBefore + 1, reallocations.get());
  }

  @Test
  public void testHashCodeEquals() {
    IntLongHashMap l0 = newInstance();
    assertEquals(0, l0.hashCode());
    assertEquals(l0, newInstance());

    IntLongHashMap l1 =
        IntLongHashMap.from(newArray(key1, key2, key3), newvArray(value1, value2, value3));

    IntLongHashMap l2 =
        IntLongHashMap.from(newArray(key2, key1, key3), newvArray(value2, value1, value3));

    IntLongHashMap l3 = IntLongHashMap.from(newArray(key1, key2), newvArray(value2, value1));

    assertEquals(l1.hashCode(), l2.hashCode());
    assertEquals(l1, l2);

    assertFalse(l1.equals(l3));
    assertFalse(l2.equals(l3));
  }

  @Test
  public void testBug_HPPC37() {
    IntLongHashMap l1 = IntLongHashMap.from(newArray(key1), newvArray(value1));

    IntLongHashMap l2 = IntLongHashMap.from(newArray(key2), newvArray(value1));

    assertFalse(l1.equals(l2));
    assertFalse(l2.equals(l1));
  }

  @Test
  public void testEmptyValue() {
    assertEquals(0L, map.put(key1, 0L));
    assertEquals(0L, map.get(key1));
    assertTrue(map.containsKey(key1));
    map.remove(key1);
    assertFalse(map.containsKey(key1));
    assertEquals(0, map.size());
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
        int key = cast(rnd.nextInt(size));
        if (rnd.nextInt(50) == 0) {
          key = 0;
        }
        long value = vcast(rnd.nextInt());

        boolean hadOldValue = map.containsKey(key);
        if (rnd.nextBoolean()) {
          long previousValue;
          if (rnd.nextBoolean()) {
            int index = map.indexOf(key);
            if (map.indexExists(index)) {
              previousValue = map.indexReplace(index, value);
            } else {
              map.indexInsert(index, key, value);
              previousValue = 0L;
            }
          } else {
            previousValue = map.put(key, value);
          }
          assertEquals(
              other.put(key, value), ((previousValue) == 0) && !hadOldValue ? null : previousValue);

          assertEquals(value, map.get(key));
          assertEquals(value, map.indexGet(map.indexOf(key)));
          assertTrue(map.containsKey(key));
          assertTrue(map.indexExists(map.indexOf(key)));
        } else {
          assertEquals(other.containsKey(key), map.containsKey(key));
          long previousValue =
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

    IntLongHashMap cloned = map.clone();
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
    for (LongCursor c : map.values()) {
      assertEquals(map.values[c.index], c.value);
      counted++;
    }
    assertEquals(counted, map.size());
  }

  /* */
  @Test
  public void testEqualsSameClass() {
    IntLongHashMap l1 = newInstance();
    l1.put(k1, value0);
    l1.put(k2, value1);
    l1.put(k3, value2);

    IntLongHashMap l2 = new IntLongHashMap(l1);
    l2.putAll(l1);

    IntLongHashMap l3 = new IntLongHashMap(l2);
    l3.putAll(l2);
    l3.put(k4, value0);

    assertEquals(l1, l2);
    assertEquals(l1.hashCode(), l2.hashCode());
    assertNotEquals(l1, l3);
  }

  /* */
  @Test
  public void testEqualsSubClass() {
    class Sub extends IntLongHashMap {}
    ;

    IntLongHashMap l1 = newInstance();
    l1.put(k1, value0);
    l1.put(k2, value1);
    l1.put(k3, value2);

    IntLongHashMap l2 = new Sub();
    l2.putAll(l1);
    l2.put(k4, value3);

    IntLongHashMap l3 = new Sub();
    l3.putAll(l2);

    assertNotEquals(l1, l2);
    assertEquals(l2.hashCode(), l3.hashCode());
    assertEquals(l2, l3);
  }
}
