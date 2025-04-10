package org.apache.lucene.codecs;

import java.util.Arrays;

/**
 * Computes bin quality metrics like balance, skewness, and Gini index.
 */
public final class BinQualityMetrics {

  private BinQualityMetrics() {}

  /**
   * Computes standard deviation of document counts across bins.
   */
  public static double computeStdDev(int[] docCounts) {
    double mean = Arrays.stream(docCounts).average().orElse(0.0);
    double variance = 0.0;
    for (int count : docCounts) {
      double delta = count - mean;
      variance += delta * delta;
    }
    return Math.sqrt(variance / docCounts.length);
  }

  /**
   * Computes the Gini index to measure bin inequality.
   */
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