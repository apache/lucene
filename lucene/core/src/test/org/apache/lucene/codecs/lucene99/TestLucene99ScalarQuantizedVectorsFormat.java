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
package org.apache.lucene.codecs.lucene99;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ScalarQuantizer;

public class TestLucene99ScalarQuantizedVectorsFormat extends LuceneTestCase {

  public void testDefaultQuantile() {
    float defaultQuantile = Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(99);
    assertEquals(0.99f, defaultQuantile, 1e-5);
    defaultQuantile = Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(1);
    assertEquals(0.9f, defaultQuantile, 1e-5);
    defaultQuantile =
        Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(Integer.MAX_VALUE - 2);
    assertEquals(1.0f, defaultQuantile, 1e-5);
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99ScalarQuantizedVectorsFormat(0.89f));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99ScalarQuantizedVectorsFormat(1.1f));
  }

  public void testQuantileMergeWithMissing() {
    List<ScalarQuantizer> quantiles = new ArrayList<>();
    quantiles.add(new ScalarQuantizer(0.1f, 0.2f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.2f, 0.3f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.3f, 0.4f, 0.1f));
    quantiles.add(null);
    List<Integer> segmentSizes = List.of(1, 1, 1, 1);
    assertNull(Lucene99ScalarQuantizedVectorsWriter.mergeQuantiles(quantiles, segmentSizes, 0.1f));
    assertNull(Lucene99ScalarQuantizedVectorsWriter.mergeQuantiles(List.of(), List.of(), 0.1f));
  }

  public void testQuantileMerge() {
    List<ScalarQuantizer> quantiles = new ArrayList<>();
    quantiles.add(new ScalarQuantizer(0.1f, 0.2f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.2f, 0.3f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.3f, 0.4f, 0.1f));
    List<Integer> segmentSizes = List.of(1, 1, 1);
    ScalarQuantizer merged =
        Lucene99ScalarQuantizedVectorsWriter.mergeQuantiles(quantiles, segmentSizes, 0.1f);
    assertEquals(0.2f, merged.getLowerQuantile(), 1e-5);
    assertEquals(0.3f, merged.getUpperQuantile(), 1e-5);
    assertEquals(0.1f, merged.getConfiguredQuantile(), 1e-5);
  }

  public void testQuantileMergeWithDifferentSegmentSizes() {
    List<ScalarQuantizer> quantiles = new ArrayList<>();
    quantiles.add(new ScalarQuantizer(0.1f, 0.2f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.2f, 0.3f, 0.1f));
    quantiles.add(new ScalarQuantizer(0.3f, 0.4f, 0.1f));
    List<Integer> segmentSizes = List.of(1, 2, 3);
    ScalarQuantizer merged =
        Lucene99ScalarQuantizedVectorsWriter.mergeQuantiles(quantiles, segmentSizes, 0.1f);
    assertEquals(0.2333333f, merged.getLowerQuantile(), 1e-5);
    assertEquals(0.3333333f, merged.getUpperQuantile(), 1e-5);
    assertEquals(0.1f, merged.getConfiguredQuantile(), 1e-5);
  }
}
