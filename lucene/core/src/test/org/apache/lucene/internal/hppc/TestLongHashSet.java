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

import static org.hamcrest.Matchers.*;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link LongHashSet}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongHashSetTest
 *
 * <p>github: https://github.com/carrotsearch/hppc release: 0.9.0
 */
public class TestLongHashSet extends LuceneTestCase {
  private static final long EMPTY_KEY = 0L;

  private final long keyE = 0;
  private final long key1 = cast(1);
  private final long key2 = cast(2);
  private final long key3 = cast(3);
  private final long key4 = cast(4);

  /** Per-test fresh initialized instance. */
  private LongHashSet set;

  /** Convert to target type from an integer used to test stuff. */
  private long cast(int v) {
    return v;
  }

  @Before
  public void initialize() {
    set = new LongHashSet();
  }

  @Test
  public void testAddAllViaInterface() {
    set.addAll(key1, key2);

    LongHashSet iface = new LongHashSet();
    iface.clear();
    iface.addAll(set);
    MatcherAssert.assertThat(set(iface.toArray()), is(equalTo(set(key1, key2))));
  }

  @Test
  public void testIndexMethods() {
    set.add(keyE);
    set.add(key1);

    MatcherAssert.assertThat(set.indexOf(keyE), is(greaterThanOrEqualTo(0)));
    MatcherAssert.assertThat(set.indexOf(key1), is(greaterThanOrEqualTo(0)));
    MatcherAssert.assertThat(set.indexOf(key2), is(lessThan(0)));

    MatcherAssert.assertThat(set.indexExists(set.indexOf(keyE)), is(true));
    MatcherAssert.assertThat(set.indexExists(set.indexOf(key1)), is(true));
    MatcherAssert.assertThat(set.indexExists(set.indexOf(key2)), is(false));

    MatcherAssert.assertThat(set.indexGet(set.indexOf(keyE)), is(equalTo(keyE)));
    MatcherAssert.assertThat(set.indexGet(set.indexOf(key1)), is(equalTo(key1)));

    expectThrows(
        AssertionError.class,
        () -> {
          set.indexGet(set.indexOf(key2));
        });

    MatcherAssert.assertThat(set.indexReplace(set.indexOf(keyE), keyE), is(equalTo(keyE)));
    MatcherAssert.assertThat(set.indexReplace(set.indexOf(key1), key1), is(equalTo(key1)));

    set.indexInsert(set.indexOf(key2), key2);
    MatcherAssert.assertThat(set.indexGet(set.indexOf(key2)), is(equalTo(key2)));
    MatcherAssert.assertThat(set.size(), is(equalTo(3)));

    set.indexRemove(set.indexOf(keyE));
    MatcherAssert.assertThat(set.size(), is(equalTo(2)));
    set.indexRemove(set.indexOf(key2));
    MatcherAssert.assertThat(set.size(), is(equalTo(1)));
    MatcherAssert.assertThat(set.indexOf(keyE), is(lessThan(0)));
    MatcherAssert.assertThat(set.indexOf(key1), is(greaterThanOrEqualTo(0)));
    MatcherAssert.assertThat(set.indexOf(key2), is(lessThan(0)));
  }

  @Test
  public void testCursorIndexIsValid() {
    set.add(keyE);
    set.add(key1);
    set.add(key2);

    for (LongCursor c : set) {
      MatcherAssert.assertThat(set.indexExists(c.index), is(true));
      MatcherAssert.assertThat(set.indexGet(c.index), is(equalTo(c.value)));
    }
  }

  @Test
  public void testEmptyKey() {
    LongHashSet set = new LongHashSet();

    boolean b = set.add(EMPTY_KEY);

    MatcherAssert.assertThat(b, is(true));
    MatcherAssert.assertThat(set.add(EMPTY_KEY), is(false));
    MatcherAssert.assertThat(set.size(), is(equalTo(1)));
    MatcherAssert.assertThat(set.isEmpty(), is(false));
    MatcherAssert.assertThat(set(set.toArray()), is(equalTo(set(EMPTY_KEY))));
    MatcherAssert.assertThat(set.contains(EMPTY_KEY), is(true));
    int index = set.indexOf(EMPTY_KEY);
    MatcherAssert.assertThat(set.indexExists(index), is(true));
    MatcherAssert.assertThat(set.indexGet(index), is(equalTo(EMPTY_KEY)));
    MatcherAssert.assertThat(set.indexReplace(index, EMPTY_KEY), is(equalTo(EMPTY_KEY)));

    if (random().nextBoolean()) {
      b = set.remove(EMPTY_KEY);
      MatcherAssert.assertThat(b, is(true));
    } else {
      set.indexRemove(index);
    }

    MatcherAssert.assertThat(set.size(), is(equalTo(0)));
    MatcherAssert.assertThat(set.isEmpty(), is(true));
    MatcherAssert.assertThat(set(set.toArray()), is(empty()));
    MatcherAssert.assertThat(set.contains(EMPTY_KEY), is(false));
    index = set.indexOf(EMPTY_KEY);
    MatcherAssert.assertThat(set.indexExists(index), is(false));

    set.indexInsert(index, EMPTY_KEY);
    set.add(key1);
    MatcherAssert.assertThat(set.size(), is(equalTo(2)));
    MatcherAssert.assertThat(set.contains(EMPTY_KEY), is(true));
    index = set.indexOf(EMPTY_KEY);
    MatcherAssert.assertThat(set.indexExists(index), is(true));
    MatcherAssert.assertThat(set.indexGet(index), is(equalTo(EMPTY_KEY)));
  }

  @Test
  public void testEnsureCapacity() {
    final AtomicInteger expands = new AtomicInteger();
    LongHashSet set =
        new LongHashSet(0) {
          @Override
          protected void allocateBuffers(int arraySize) {
            super.allocateBuffers(arraySize);
            expands.incrementAndGet();
          }
        };

    // Add some elements.
    final int max = rarely() ? 0 : randomIntBetween(0, 250);
    for (int i = 0; i < max; i++) {
      set.add(cast(i));
    }

    final int additions = randomIntBetween(max, max + 5000);
    set.ensureCapacity(additions + set.size());
    final int before = expands.get();
    for (int i = 0; i < additions; i++) {
      set.add(cast(i));
    }
    assertEquals(before, expands.get());
  }

  @Test
  public void testInitiallyEmpty() {
    assertEquals(0, set.size());
  }

  @Test
  public void testAdd() {
    assertTrue(set.add(key1));
    assertFalse(set.add(key1));
    assertEquals(1, set.size());
  }

  @Test
  public void testAdd2() {
    set.addAll(key1, key1);
    assertEquals(1, set.size());
    assertEquals(1, set.addAll(key1, key2));
    assertEquals(2, set.size());
  }

  @Test
  public void testAddVarArgs() {
    set.addAll(asArray(0, 1, 2, 1, 0));
    assertEquals(3, set.size());
    assertSortedListEquals(set.toArray(), asArray(0, 1, 2));
  }

  @Test
  public void testAddAll() {
    LongHashSet set2 = new LongHashSet();
    set2.addAll(asArray(1, 2));
    set.addAll(asArray(0, 1));

    assertEquals(1, set.addAll(set2));
    assertEquals(0, set.addAll(set2));

    assertEquals(3, set.size());
    assertSortedListEquals(set.toArray(), asArray(0, 1, 2));
  }

  @Test
  public void testRemove() {
    set.addAll(asArray(0, 1, 2, 3, 4));

    assertTrue(set.remove(key2));
    assertFalse(set.remove(key2));
    assertEquals(4, set.size());
    assertSortedListEquals(set.toArray(), asArray(0, 1, 3, 4));
  }

  @Test
  public void testInitialCapacityAndGrowth() {
    for (int i = 0; i < 256; i++) {
      LongHashSet set = new LongHashSet(i);

      for (int j = 0; j < i; j++) {
        set.add(cast(j));
      }

      assertEquals(i, set.size());
    }
  }

  @Test
  public void testBug_HPPC73_FullCapacityGet() {
    final AtomicInteger reallocations = new AtomicInteger();
    final int elements = 0x7F;
    set =
        new LongHashSet(elements, 1f) {
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
      set.add(cast(i));
    }

    // Non-existent key.
    long outOfSet = cast(elements + 1);
    set.remove(outOfSet);
    assertFalse(set.contains(outOfSet));
    assertEquals(reallocationsBefore, reallocations.get());

    // Should not expand because we're replacing an existing element.
    assertFalse(set.add(key1));
    assertEquals(reallocationsBefore, reallocations.get());

    // Remove from a full set.
    set.remove(key1);
    assertEquals(reallocationsBefore, reallocations.get());
    set.add(key1);

    // Check expand on "last slot of a full map" condition.
    set.add(outOfSet);
    assertEquals(reallocationsBefore + 1, reallocations.get());
  }

  @Test
  public void testRemoveAllFromLookupContainer() {
    set.addAll(asArray(0, 1, 2, 3, 4));

    LongHashSet list2 = new LongHashSet();
    list2.addAll(asArray(1, 3, 5));

    assertEquals(2, set.removeAll(list2));
    assertEquals(3, set.size());
    assertSortedListEquals(set.toArray(), asArray(0, 2, 4));
  }

  @Test
  public void testClear() {
    set.addAll(asArray(1, 2, 3));
    set.clear();
    assertEquals(0, set.size());
  }

  @Test
  public void testRelease() {
    set.addAll(asArray(1, 2, 3));
    set.release();
    assertEquals(0, set.size());
    set.addAll(asArray(1, 2, 3));
    assertEquals(3, set.size());
  }

  @Test
  public void testIterable() {
    set.addAll(asArray(1, 2, 2, 3, 4));
    set.remove(key2);
    assertEquals(3, set.size());

    int count = 0;
    for (LongCursor cursor : set) {
      count++;
      assertTrue(set.contains(cursor.value));
    }
    assertEquals(count, set.size());

    set.clear();
    assertFalse(set.iterator().hasNext());
  }

  /** Runs random insertions/deletions/clearing and compares the results against {@link HashSet}. */
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testAgainstHashSet() {
    final Random rnd = RandomizedTest.getRandom();
    final HashSet other = new HashSet();

    for (int size = 1000; size < 20000; size += 4000) {
      other.clear();
      set.clear();

      for (int round = 0; round < size * 20; round++) {
        long key = cast(rnd.nextInt(size));
        if (rnd.nextInt(50) == 0) {
          key = 0L;
        }

        if (rnd.nextBoolean()) {
          if (rnd.nextBoolean()) {
            int index = set.indexOf(key);
            if (set.indexExists(index)) {
              set.indexReplace(index, key);
            } else {
              set.indexInsert(index, key);
            }
          } else {
            set.add(key);
          }
          other.add(key);

          assertTrue(set.contains(key));
          assertTrue(set.indexExists(set.indexOf(key)));
        } else {
          assertEquals(other.contains(key), set.contains(key));
          boolean removed;
          if (set.contains(key) && rnd.nextBoolean()) {
            set.indexRemove(set.indexOf(key));
            removed = true;
          } else {
            removed = set.remove(key);
          }
          assertEquals(other.remove(key), removed);
        }

        assertEquals(other.size(), set.size());
      }
    }
  }

  @Test
  public void testHashCodeEquals() {
    LongHashSet l0 = new LongHashSet();
    assertEquals(0, l0.hashCode());
    assertEquals(l0, new LongHashSet());

    LongHashSet l1 = LongHashSet.from(key1, key2, key3);
    LongHashSet l2 = LongHashSet.from(key1, key2);
    l2.add(key3);

    assertEquals(l1.hashCode(), l2.hashCode());
    assertEquals(l1, l2);
  }

  @Test
  public void testClone() {
    this.set.addAll(asArray(1, 2, 3));

    LongHashSet cloned = set.clone();
    cloned.remove(key1);

    assertSortedListEquals(set.toArray(), asArray(1, 2, 3));
    assertSortedListEquals(cloned.toArray(), asArray(2, 3));
  }

  @Test
  public void testEqualsSameClass() {
    LongHashSet l1 = LongHashSet.from(key1, key2, key3);
    LongHashSet l2 = LongHashSet.from(key1, key2, key3);
    LongHashSet l3 = LongHashSet.from(key1, key2, key4);

    MatcherAssert.assertThat(l1, is(equalTo(l2)));
    MatcherAssert.assertThat(l1.hashCode(), is(equalTo(l2.hashCode())));
    MatcherAssert.assertThat(l1, is(not(equalTo(l3))));
  }

  @Test
  public void testEqualsSubClass() {
    class Sub extends LongHashSet {}
    ;

    LongHashSet l1 = LongHashSet.from(key1, key2, key3);
    LongHashSet l2 = new Sub();
    LongHashSet l3 = new Sub();
    l2.addAll(l1);
    l3.addAll(l1);

    MatcherAssert.assertThat(l2, is(equalTo(l3)));
    MatcherAssert.assertThat(l1, is(not(equalTo(l2))));
  }

  private static int randomIntBetween(int min, int max) {
    return min + random().nextInt(max + 1 - min);
  }

  private static Set<Long> set(long... elements) {
    Set<Long> set = new HashSet<>();
    for (long element : elements) {
      set.add(element);
    }
    return set;
  }

  private static long[] asArray(long... elements) {
    return elements;
  }

  /** Check if the array's content is identical to a given sequence of elements. */
  private static void assertSortedListEquals(long[] array, long[] elements) {
    assertEquals(elements.length, array.length);
    Arrays.sort(array);
    assertArrayEquals(elements, array);
  }
}
