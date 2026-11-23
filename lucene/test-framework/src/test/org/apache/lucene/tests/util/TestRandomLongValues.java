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
package org.apache.lucene.tests.util;

import java.util.function.BiConsumer;
import org.apache.lucene.tests.util.RandomLongValues.Bounds;
import org.apache.lucene.tests.util.RandomLongValues.Distribution;

public class TestRandomLongValues extends LuceneTestCase {

  public void testValuesStayWithinBounds() {
    forEachDistribution((distribution, values) -> {
      for (int i = 0; i < 200; i++) {
        long value = values.nextValue();
        assertTrue(distribution + " " + value, values.bounds().contains(value));
      }
    });
  }

  public void testContainingBounds() {
    forEachDistribution((distribution, values) -> {
      for (int i = 0; i < 200; i++) {
        Bounds bound = values.nextContainingBound();
        assertTrue(distribution + " " + bound, bound.contains(values.bounds()));
      }
    });
  }

  public void testContainedBounds() {
    forEachDistribution((distribution, values) -> {
      for (int i = 0; i < 200; i++) {
        Bounds bound = values.nextContainedBound();
        assertTrue(distribution + " " + bound, values.bounds().contains(bound));
      }
    });
  }

  public void testOverlappingBounds() {
    forEachDistribution((distribution, values) -> {
      for (int i = 0; i < 200; i++) {
        Bounds bound = values.nextOverlappingBound();
        assertTrue(distribution + " " + bound, values.bounds().overlaps(bound));
      }
    });
  }

  public void testConstantHasSingleValue() {
    RandomLongValues values = RandomLongValues.of(random(), Distribution.CONSTANT);
    assertEquals(values.bounds().min(), values.bounds().max());
    assertEquals(values.bounds().min(), values.nextValue());
  }

  public void testBoundsRejectInvertedRange() {
    expectThrows(IllegalArgumentException.class, () -> new Bounds(2, 1));
  }

  private void forEachDistribution(BiConsumer<Distribution, RandomLongValues> check) {
    for (Distribution distribution : Distribution.values()) {
      for (int iter = 0; iter < 50; iter++) {
        RandomLongValues values = RandomLongValues.of(random(), distribution);
        check.accept(distribution, values);
        values.nextBound();
      }
    }
  }
}
