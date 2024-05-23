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
package org.apache.lucene.document;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDocValuesLongHashSet extends LuceneTestCase {

  private void assertEquals(Set<Long> set1, DocValuesLongHashSet longHashSet) {
    assertEquals(set1.size(), longHashSet.size());

    Set<Long> set2 = longHashSet.stream().boxed().collect(Collectors.toSet());
    LuceneTestCase.assertEquals(set1, set2);

    if (set1.isEmpty() == false) {
      Set<Long> set3 = new HashSet<>(set1);
      long removed = set3.iterator().next();
      while (true) {
        long next = random().nextLong();
        if (next != removed && set3.add(next)) {
          assertFalse(longHashSet.contains(next));
          break;
        }
      }
      assertNotEquals(set3, longHashSet);
    }

    assertTrue(set1.stream().allMatch(longHashSet::contains));
  }

  private void assertNotEquals(Set<Long> set1, DocValuesLongHashSet longHashSet) {
    Set<Long> set2 = longHashSet.stream().boxed().collect(Collectors.toSet());

    LuceneTestCase.assertNotEquals(set1, set2);

    DocValuesLongHashSet set3 =
        new DocValuesLongHashSet(set1.stream().mapToLong(Long::longValue).sorted().toArray());

    LuceneTestCase.assertNotEquals(set2, set3.stream().boxed().collect(Collectors.toSet()));

    assertFalse(set1.stream().allMatch(longHashSet::contains));
  }

  public void testEmpty() {
    Set<Long> set1 = new HashSet<>();
    DocValuesLongHashSet set2 = new DocValuesLongHashSet(new long[] {});
    assertEquals(0, set2.size());
    assertEquals(Long.MAX_VALUE, set2.minValue);
    assertEquals(Long.MIN_VALUE, set2.maxValue);
    assertEquals(set1, set2);
  }

  public void testOneValue() {
    Set<Long> set1 = new HashSet<>(Arrays.asList(42L));
    DocValuesLongHashSet set2 = new DocValuesLongHashSet(new long[] {42L});
    assertEquals(1, set2.size());
    assertEquals(42L, set2.minValue);
    assertEquals(42L, set2.maxValue);
    assertEquals(set1, set2);

    set1 = new HashSet<>(Arrays.asList(Long.MIN_VALUE));
    set2 = new DocValuesLongHashSet(new long[] {Long.MIN_VALUE});
    assertEquals(1, set2.size());
    assertEquals(Long.MIN_VALUE, set2.minValue);
    assertEquals(Long.MIN_VALUE, set2.maxValue);
    assertEquals(set1, set2);
  }

  public void testTwoValues() {
    Set<Long> set1 = new HashSet<>(Arrays.asList(42L, Long.MAX_VALUE));
    DocValuesLongHashSet set2 = new DocValuesLongHashSet(new long[] {42L, Long.MAX_VALUE});
    assertEquals(2, set2.size());
    assertEquals(42, set2.minValue);
    assertEquals(Long.MAX_VALUE, set2.maxValue);
    assertEquals(set1, set2);

    set1 = new HashSet<>(Arrays.asList(Long.MIN_VALUE, 42L));
    set2 = new DocValuesLongHashSet(new long[] {Long.MIN_VALUE, 42L});
    assertEquals(2, set2.size());
    assertEquals(Long.MIN_VALUE, set2.minValue);
    assertEquals(42, set2.maxValue);
    assertEquals(set1, set2);
  }

  public void testSameValue() {
    DocValuesLongHashSet set2 = new DocValuesLongHashSet(new long[] {42L, 42L});
    assertEquals(1, set2.size());
    assertEquals(42L, set2.minValue);
    assertEquals(42L, set2.maxValue);
  }

  public void testSameMissingPlaceholder() {
    DocValuesLongHashSet set2 =
        new DocValuesLongHashSet(new long[] {Long.MIN_VALUE, Long.MIN_VALUE});
    assertEquals(1, set2.size());
    assertEquals(Long.MIN_VALUE, set2.minValue);
    assertEquals(Long.MIN_VALUE, set2.maxValue);
  }

  public void testRandom() {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      long[] values = new long[random().nextInt(1 << random().nextInt(16))];
      for (int i = 0; i < values.length; ++i) {
        if (i == 0 || random().nextInt(10) < 9) {
          values[i] = random().nextLong();
        } else {
          values[i] = values[random().nextInt(i)];
        }
      }
      if (values.length > 0 && random().nextBoolean()) {
        values[values.length / 2] = Long.MIN_VALUE;
      }
      Set<Long> set1 = LongStream.of(values).mapToObj(Long::valueOf).collect(Collectors.toSet());
      Arrays.sort(values);
      DocValuesLongHashSet set2 = new DocValuesLongHashSet(values);
      assertEquals(set1, set2);
    }
  }
}
