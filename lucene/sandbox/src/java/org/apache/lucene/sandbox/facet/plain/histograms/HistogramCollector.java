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
package org.apache.lucene.sandbox.facet.plain.histograms;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

final class HistogramCollector implements Collector {

  private final String field;
  private final long interval;
  private final int maxBuckets;
  private final LongIntHashMap counts;

  HistogramCollector(String field, long interval, int maxBuckets) {
    this.field = field;
    this.interval = interval;
    this.maxBuckets = maxBuckets;
    this.counts = new LongIntHashMap();
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // The segment has no values, nothing to do.
      throw new CollectionTerminatedException();
    }
    if (fi.getDocValuesType() != DocValuesType.NUMERIC
        && fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
      throw new IllegalStateException(
          "Expected numeric field, but got doc-value type: " + fi.getDocValuesType());
    }
    SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
    NumericDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton == null) {
      return new HistogramNaiveLeafCollector(values, interval, maxBuckets, counts);
    } else {
      DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
      if (skipper != null) {
        long leafMinQuotient = Math.floorDiv(skipper.minValue(), interval);
        long leafMaxQuotient = Math.floorDiv(skipper.maxValue(), interval);
        if (leafMaxQuotient - leafMinQuotient <= 1024) {
          // Only use the optimized implementation if there is a small number of unique quotients,
          // so that we can count them using a dense array instead of a hash table.
          return new HistogramLeafCollector(singleton, skipper, interval, maxBuckets, counts);
        }
      }
      return new HistogramNaiveSingleValuedLeafCollector(singleton, interval, maxBuckets, counts);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  LongIntHashMap getCounts() {
    return counts;
  }

  /**
   * Naive implementation of a histogram {@link LeafCollector}, which iterates all maches and looks
   * up the value to determine the corresponding bucket.
   */
  private static class HistogramNaiveLeafCollector implements LeafCollector {

    private final SortedNumericDocValues values;
    private final long interval;
    private final int maxBuckets;
    private final LongIntHashMap counts;

    HistogramNaiveLeafCollector(
        SortedNumericDocValues values, long interval, int maxBuckets, LongIntHashMap counts) {
      this.values = values;
      this.interval = interval;
      this.maxBuckets = maxBuckets;
      this.counts = counts;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        int valueCount = values.docValueCount();
        long prevQuotient = Long.MIN_VALUE;
        for (int i = 0; i < valueCount; ++i) {
          final long value = values.nextValue();
          final long quotient = Math.floorDiv(value, interval);
          // We must not double-count values that divide to the same quotient since this returns doc
          // counts as opposed to value counts.
          if (quotient != prevQuotient) {
            counts.addTo(quotient, 1);
            checkMaxBuckets(counts.size(), maxBuckets);
            prevQuotient = quotient;
          }
        }
      }
    }
  }

  /**
   * Naive implementation of a histogram {@link LeafCollector}, which iterates all maches and looks
   * up the value to determine the corresponding bucket.
   */
  private static class HistogramNaiveSingleValuedLeafCollector implements LeafCollector {

    private final NumericDocValues values;
    private final long interval;
    private final int maxBuckets;
    private final LongIntHashMap counts;

    HistogramNaiveSingleValuedLeafCollector(
        NumericDocValues values, long interval, int maxBuckets, LongIntHashMap counts) {
      this.values = values;
      this.interval = interval;
      this.maxBuckets = maxBuckets;
      this.counts = counts;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        final long value = values.longValue();
        final long quotient = Math.floorDiv(value, interval);
        counts.addTo(quotient, 1);
        checkMaxBuckets(counts.size(), maxBuckets);
      }
    }
  }

  /**
   * Optimized histogram {@link LeafCollector}, that takes advantage of the doc-values index to
   * speed up collection.
   */
  private static class HistogramLeafCollector implements LeafCollector {

    private final NumericDocValues values;
    private final DocValuesSkipper skipper;
    private final long interval;
    private final int maxBuckets;
    private final int[] counts;
    private final long leafMinQuotient;
    private final LongIntHashMap collectorCounts;

    /**
     * Max doc ID (inclusive) up to which all docs may map to values that have the same quotient.
     */
    private int upToInclusive = -1;

    /** Whether all docs up to {@link #upToInclusive} map to values that have the same quotient. */
    private boolean upToSameQuotient;

    /** Index in {@link #counts} for docs up to {@link #upToInclusive}. */
    private int upToQuotientIndex;

    HistogramLeafCollector(
        NumericDocValues values,
        DocValuesSkipper skipper,
        long interval,
        int maxBuckets,
        LongIntHashMap collectorCounts) {
      this.values = values;
      this.skipper = skipper;
      this.interval = interval;
      this.maxBuckets = maxBuckets;
      this.collectorCounts = collectorCounts;

      leafMinQuotient = Math.floorDiv(skipper.minValue(), interval);
      long leafMaxQuotient = Math.floorDiv(skipper.maxValue(), interval);
      counts = new int[Math.toIntExact(leafMaxQuotient - leafMinQuotient + 1)];
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    private void advanceSkipper(int doc) throws IOException {
      if (doc > skipper.maxDocID(0)) {
        skipper.advance(doc);
      }
      upToSameQuotient = false;

      if (skipper.minDocID(0) > doc) {
        // Corner case which happens if `doc` doesn't have a value and is between two intervals of
        // the doc-value skip index.
        upToInclusive = skipper.minDocID(0) - 1;
        return;
      }

      upToInclusive = skipper.maxDocID(0);

      // Now find the highest level where all docs have the same quotient.
      for (int level = 0; level < skipper.numLevels(); ++level) {
        int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
        long minQuotient = Math.floorDiv(skipper.minValue(level), interval);
        long maxQuotient = Math.floorDiv(skipper.maxValue(level), interval);

        if (skipper.docCount(level) == totalDocsAtLevel && minQuotient == maxQuotient) {
          // All docs at this level have a value, and all values map to the same quotient.
          upToInclusive = skipper.maxDocID(level);
          upToSameQuotient = true;
          upToQuotientIndex = (int) (minQuotient - this.leafMinQuotient);
        } else {
          break;
        }
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      if (doc > upToInclusive) {
        advanceSkipper(doc);
      }

      if (upToSameQuotient) {
        counts[upToQuotientIndex]++;
      } else if (values.advanceExact(doc)) {
        final long value = values.longValue();
        final long quotient = Math.floorDiv(value, interval);
        counts[(int) (quotient - leafMinQuotient)]++;
      }
    }

    @Override
    public void finish() throws IOException {
      // Put counts that we computed in the int[] back into the hash map.
      for (int i = 0; i < counts.length; ++i) {
        collectorCounts.addTo(leafMinQuotient + i, counts[i]);
      }
      checkMaxBuckets(collectorCounts.size(), maxBuckets);
    }
  }

  private static void checkMaxBuckets(int size, int maxBuckets) {
    if (size > maxBuckets) {
      throw new IllegalStateException(
          "Collected "
              + size
              + " buckets, which is more than the configured max number of buckets: "
              + maxBuckets);
    }
  }
}
