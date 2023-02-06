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

public class TestLongHashSet extends LuceneTestCase {

  private void assertEquals(Set<Long> set1, LongHashSet longHashSet) {
    Set<Long> set2 = longHashSet.toSet();

    LuceneTestCase.assertEquals(set1, set2);

    if (set1.isEmpty() == false) {
      Set<Long> set3 = new HashSet<>(set1);
      long removed = set3.iterator().next();
      while (true) {
        long next = random().nextLong();
        if (next != removed && set3.add(next)) {
          break;
        }
      }
      assertNotEquals(set3, longHashSet);
    }
  }

  private void assertNotEquals(Set<Long> set1, LongHashSet longHashSet) {
    Set<Long> set2 = longHashSet.toSet();

    LuceneTestCase.assertNotEquals(set1, set2);

    LongHashSet set3 = new LongHashSet(set1.stream().mapToLong(Long::longValue).sorted().toArray());

    LuceneTestCase.assertNotEquals(set2, set3.toSet());
  }

  public void testEmpty() {
    Set<Long> set1 = new HashSet<>();
    LongHashSet set2 = new LongHashSet(new long[] {});
    assertEquals(Long.MAX_VALUE, set2.minValue);
    assertEquals(Long.MIN_VALUE, set2.maxValue);
    assertEquals(set1, set2);
  }

  public void testOneValue() {
    Set<Long> set1 = new HashSet<>(Arrays.asList(42L));
    LongHashSet set2 = new LongHashSet(new long[] {42L});
    assertEquals(42L, set2.minValue);
    assertEquals(42L, set2.maxValue);
    assertEquals(set1, set2);

    set1 = new HashSet<>(Arrays.asList(Long.MIN_VALUE));
    set2 = new LongHashSet(new long[] {Long.MIN_VALUE});
    assertEquals(Long.MIN_VALUE, set2.minValue);
    assertEquals(Long.MIN_VALUE, set2.maxValue);
    assertEquals(set1, set2);
  }

  public void testTwoValues() {
    Set<Long> set1 = new HashSet<>(Arrays.asList(42L, Long.MAX_VALUE));
    LongHashSet set2 = new LongHashSet(new long[] {42L, Long.MAX_VALUE});
    assertEquals(42, set2.minValue);
    assertEquals(Long.MAX_VALUE, set2.maxValue);
    assertEquals(set1, set2);

    set1 = new HashSet<>(Arrays.asList(Long.MIN_VALUE, 42L));
    set2 = new LongHashSet(new long[] {Long.MIN_VALUE, 42L});
    assertEquals(Long.MIN_VALUE, set2.minValue);
    assertEquals(42, set2.maxValue);
    assertEquals(set1, set2);
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
      Set<Long> set1 =
          LongStream.of(values)
              .mapToObj(Long::valueOf)
              .collect(Collectors.toCollection(HashSet::new));
      Arrays.sort(values);
      LongHashSet set2 = new LongHashSet(values);
      assertEquals(set1, set2);
    }
  }
}
