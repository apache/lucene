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

import java.util.Arrays;

/** Computes bin quality metrics like balance, skewness, and Gini index. */
public final class BinQualityMetrics {

  private BinQualityMetrics() {}

  /** Computes standard deviation of document counts across bins. */
  public static double computeStdDev(int[] docCounts) {
    double mean = Arrays.stream(docCounts).average().orElse(0.0);
    double variance = 0.0;
    for (int count : docCounts) {
      double delta = count - mean;
      variance += delta * delta;
    }
    return Math.sqrt(variance / docCounts.length);
  }

  /** Computes the Gini index to measure bin inequality. */
  public static double computeGiniIndex(int[] docCounts) {
    int n = docCounts.length;
    if (n == 0) return 0.0;
    int[] sorted = docCounts.clone();
    Arrays.sort(sorted);
    double cumulative = 0.0;
    for (int i = 0; i < n; i++) {
      cumulative += (i + 1L) * sorted[i];
    }
    double total = Arrays.stream(sorted).sum();
    if (total == 0.0) return 0.0;
    return (2.0 * cumulative) / (n * total) - (n + 1.0) / n;
  }
}
