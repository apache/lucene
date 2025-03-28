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
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

final class HistogramCollector implements Collector {

  private final String field;
  private final long bucketWidth;
  private final int maxBuckets;
  private final LongIntHashMap counts;

  HistogramCollector(String field, long bucketWidth, int maxBuckets) {
    this.field = field;
    this.bucketWidth = bucketWidth;
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
      return new HistogramNaiveLeafCollector(values, bucketWidth, maxBuckets, counts);
    } else {
      DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
      if (skipper != null) {
        long leafMinBucket = Math.floorDiv(skipper.minValue(), bucketWidth);
        long leafMaxBucket = Math.floorDiv(skipper.maxValue(), bucketWidth);
        if (leafMaxBucket - leafMinBucket <= 1024) {
          // Only use the optimized implementation if there is a small number of unique buckets,
          // so that we can count them using a dense array instead of a hash table. This helps save
          // the overhead of hashing and collision resolution.
          return new HistogramLeafCollector(singleton, skipper, bucketWidth, maxBuckets, counts);
        }
      }
      return new HistogramNaiveSingleValuedLeafCollector(
          singleton, bucketWidth, maxBuckets, counts);
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
    private final long bucketWidth;
    private final int maxBuckets;
    private final LongIntHashMap counts;

    HistogramNaiveLeafCollector(
        SortedNumericDocValues values, long bucketWidth, int maxBuckets, LongIntHashMap counts) {
      this.values = values;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;
      this.counts = counts;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        int valueCount = values.docValueCount();
        long prevBucket = Long.MIN_VALUE;
        for (int i = 0; i < valueCount; ++i) {
          final long value = values.nextValue();
          final long bucket = Math.floorDiv(value, bucketWidth);
          // We must not double-count values that map to the same bucket since this returns doc
          // counts as opposed to value counts.
          if (bucket != prevBucket) {
            counts.addTo(bucket, 1);
            checkMaxBuckets(counts.size(), maxBuckets);
            prevBucket = bucket;
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
    private final long bucketWidth;
    private final int maxBuckets;
    private final LongIntHashMap counts;

    HistogramNaiveSingleValuedLeafCollector(
        NumericDocValues values, long bucketWidth, int maxBuckets, LongIntHashMap counts) {
      this.values = values;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;
      this.counts = counts;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        final long value = values.longValue();
        final long bucket = Math.floorDiv(value, bucketWidth);
        counts.addTo(bucket, 1);
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
    private final long bucketWidth;
    private final int maxBuckets;
    private final int[] counts;
    private final long leafMinBucket;
    private final LongIntHashMap collectorCounts;

    /** Max doc ID (inclusive) up to which all docs values may map to the same bucket. */
    private int upToInclusive = -1;

    /** Whether all docs up to {@link #upToInclusive} values map to the same bucket. */
    private boolean upToSameBucket;

    /** Index in {@link #counts} for docs up to {@link #upToInclusive}. */
    private int upToBucketIndex;

    HistogramLeafCollector(
        NumericDocValues values,
        DocValuesSkipper skipper,
        long bucketWidth,
        int maxBuckets,
        LongIntHashMap collectorCounts) {
      this.values = values;
      this.skipper = skipper;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;
      this.collectorCounts = collectorCounts;

      leafMinBucket = Math.floorDiv(skipper.minValue(), bucketWidth);
      long leafMaxBucket = Math.floorDiv(skipper.maxValue(), bucketWidth);
      counts = new int[Math.toIntExact(leafMaxBucket - leafMinBucket + 1)];
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    private void advanceSkipper(int doc) throws IOException {
      if (doc > skipper.maxDocID(0)) {
        skipper.advance(doc);
      }
      upToSameBucket = false;

      if (skipper.minDocID(0) > doc) {
        // Corner case which happens if `doc` doesn't have a value and is between two intervals of
        // the doc-value skip index.
        upToInclusive = skipper.minDocID(0) - 1;
        return;
      }

      upToInclusive = skipper.maxDocID(0);

      // Now find the highest level where all docs map to the same bucket.
      for (int level = 0; level < skipper.numLevels(); ++level) {
        int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
        long minBucket = Math.floorDiv(skipper.minValue(level), bucketWidth);
        long maxBucket = Math.floorDiv(skipper.maxValue(level), bucketWidth);

        if (skipper.docCount(level) == totalDocsAtLevel && minBucket == maxBucket) {
          // All docs at this level have a value, and all values map to the same bucket.
          upToInclusive = skipper.maxDocID(level);
          upToSameBucket = true;
          upToBucketIndex = (int) (minBucket - this.leafMinBucket);
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

      if (upToSameBucket) {
        counts[upToBucketIndex]++;
      } else if (values.advanceExact(doc)) {
        final long value = values.longValue();
        final long bucket = Math.floorDiv(value, bucketWidth);
        counts[(int) (bucket - leafMinBucket)]++;
      }
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      for (; ; ) {
        int upToExclusive = upToInclusive + 1;
        if (upToExclusive < 0) { // overflow
          upToExclusive = Integer.MAX_VALUE;
        }

        if (upToSameBucket) {
          counts[upToBucketIndex] += stream.count(upToExclusive);
        } else {
          stream.forEach(upToExclusive, this::collect);
        }

        if (stream.mayHaveRemaining()) {
          advanceSkipper(upToExclusive);
        } else {
          break;
        }
      }
    }

    @Override
    public void finish() throws IOException {
      // Put counts that we computed in the int[] back into the hash map.
      for (int i = 0; i < counts.length; ++i) {
        if (counts[i] != 0) {
          collectorCounts.addTo(leafMinBucket + i, counts[i]);
        }
      }
      checkMaxBuckets(collectorCounts.size(), maxBuckets);
    }
  }

  static void checkMaxBuckets(int size, int maxBuckets) {
    if (size > maxBuckets) {
      throw new IllegalStateException(
          "Collected "
              + size
              + " buckets, which is more than the configured max number of buckets: "
              + maxBuckets);
    }
  }
}
