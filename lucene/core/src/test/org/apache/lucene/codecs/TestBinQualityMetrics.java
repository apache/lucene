/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Unit tests for verifying bin balance and quality. */
public class TestBinQualityMetrics extends LuceneTestCase {

  public void testStandardDeviationAndGini() {
    int[] counts = new int[] {100, 90, 110, 95, 105}; // reasonably balanced
    double stddev = BinQualityMetrics.computeStdDev(counts);
    double gini = BinQualityMetrics.computeGiniIndex(counts);
    assertTrue("Expected low stddev", stddev < 10.0);
    assertTrue("Expected low Gini", gini < 0.05);
  }

  public void testUnbalancedBins() {
    int[] counts = new int[] {300, 0, 0, 0, 0}; // extremely skewed
    double stddev = BinQualityMetrics.computeStdDev(counts);
    double gini = BinQualityMetrics.computeGiniIndex(counts);
    assertTrue("Expected high stddev", stddev > 100.0);
    assertTrue("Expected high Gini", gini >= 0.8);
  }

  public void testRandomBins() {
    Random random = new Random(42);
    int[] counts = new int[16];
    for (int i = 0; i < counts.length; i++) {
      counts[i] = random.nextInt(100);
    }
    double gini = BinQualityMetrics.computeGiniIndex(counts);
    assertTrue("Gini index must be in range [0,1]", gini >= 0.0 && gini <= 1.0);
  }
}
