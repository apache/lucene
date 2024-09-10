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
package org.apache.lucene.facet.range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * Methods to create dynamic ranges for numeric fields.
 *
 * @lucene.experimental
 */
public final class DynamicRangeUtil {

  private DynamicRangeUtil() {}

  /**
   * Construct dynamic ranges using the specified weight field to generate equi-weight range for the
   * specified numeric bin field
   *
   * @param weightFieldName Name of the specified weight field
   * @param weightValueSource Value source of the weight field
   * @param fieldValueSource Value source of the value field
   * @param facetsCollector FacetsCollector
   * @param topN Number of requested ranges
   * @param exec An executor service that is used to do the computation
   * @return A list of DynamicRangeInfo that contains count, relevance, min, max, and centroid for
   *     each range
   */
  public static List<DynamicRangeInfo> computeDynamicRanges(
      String weightFieldName,
      LongValuesSource weightValueSource,
      LongValuesSource fieldValueSource,
      FacetsCollector facetsCollector,
      int topN,
      ExecutorService exec)
      throws IOException {

    List<FacetsCollector.MatchingDocs> matchingDocsList = facetsCollector.getMatchingDocs();
    int totalDoc = matchingDocsList.stream().mapToInt(matchingDoc -> matchingDoc.totalHits).sum();
    long[] values = new long[totalDoc];
    long[] weights = new long[totalDoc];
    long totalWeight = 0;
    int overallLength = 0;

    List<Future<?>> futures = new ArrayList<>();
    List<SegmentTask> tasks = new ArrayList<>();
    for (FacetsCollector.MatchingDocs matchingDocs : matchingDocsList) {
      if (matchingDocs.totalHits > 0) {
        SegmentOutput segmentOutput = new SegmentOutput(matchingDocs.totalHits);

        // [1] retrieve values and associated weights concurrently
        SegmentTask task =
            new SegmentTask(matchingDocs, fieldValueSource, weightValueSource, segmentOutput);
        tasks.add(task);
        futures.add(exec.submit(task));
      }
    }

    // [2] wait for all segment runs to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      } catch (ExecutionException ee) {
        IOUtils.rethrowAlways(ee.getCause());
      }
    }

    // [3] merge the segment value and weight arrays into one array respectively and update the
    // total weights
    // and valid value length
    for (SegmentTask task : tasks) {
      SegmentOutput curSegmentOutput = task.segmentOutput;
      // if segment total weight overflows, return null
      if (curSegmentOutput == null) {
        return null;
      }

      assert curSegmentOutput.values.length == curSegmentOutput.weights.length;

      try {
        totalWeight = Math.addExact(curSegmentOutput.segmentTotalWeight, totalWeight);
      } catch (ArithmeticException ae) {
        throw new IllegalArgumentException(
            "weight field \"" + weightFieldName + "\": long totalWeight value out of bounds", ae);
      }

      int currSegmentLen = curSegmentOutput.segmentIdx;
      System.arraycopy(curSegmentOutput.values, 0, values, overallLength, currSegmentLen);
      System.arraycopy(curSegmentOutput.weights, 0, weights, overallLength, currSegmentLen);
      overallLength += currSegmentLen;
    }
    return computeDynamicNumericRanges(values, weights, overallLength, totalWeight, topN);
  }

  private static class SegmentTask implements Callable<Void> {
    private final FacetsCollector.MatchingDocs matchingDocs;
    private final DocIdSetIterator matchingParentDocsItr;
    private final LongValuesSource fieldValueSource;
    private final LongValuesSource weightValueSource;
    private SegmentOutput segmentOutput;

    SegmentTask(
        FacetsCollector.MatchingDocs matchingDocs,
        LongValuesSource fieldValueSource,
        LongValuesSource weightValueSource,
        SegmentOutput segmentOutput)
        throws IOException {
      this.matchingDocs = matchingDocs;
      this.matchingParentDocsItr = matchingDocs.bits.iterator();
      this.fieldValueSource = fieldValueSource;
      this.weightValueSource = weightValueSource;
      this.segmentOutput = segmentOutput;
    }

    @Override
    public Void call() throws Exception {
      LongValues fieldValue = fieldValueSource.getValues(matchingDocs.context, null);
      LongValues weightValue = weightValueSource.getValues(matchingDocs.context, null);
      for (int doc = matchingParentDocsItr.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = matchingParentDocsItr.nextDoc()) {
        // If this doc doesn't have a weight, we skip it.
        if (fieldValue.advanceExact(doc) == false || weightValue.advanceExact(doc) == false) {
          continue;
        }

        long curValue = fieldValue.longValue();

        long curWeight = weightValue.longValue();
        // We skip weights equal to zero, otherwise they can skew the ranges.
        // Imagine all the weights were zero - any ranges would be valid.
        if (curWeight == 0) {
          continue;
        }

        segmentOutput.values[segmentOutput.segmentIdx] = curValue;
        segmentOutput.weights[segmentOutput.segmentIdx] = curWeight;
        try {
          segmentOutput.segmentTotalWeight =
              Math.addExact(segmentOutput.segmentTotalWeight, curWeight);
        } catch (ArithmeticException ae) {
          throw new IllegalArgumentException("segment long totalWeight value out of bounds", ae);
        }
        segmentOutput.segmentIdx++;
      }
      return null;
    }
  }

  /** Holds field value array, weight array, totalWeight, valid value index for each segment */
  private static final class SegmentOutput {
    private final long[] values;
    private final long[] weights;
    private long segmentTotalWeight;
    private int segmentIdx;

    public SegmentOutput(int hitsLength) {
      this.values = new long[hitsLength];
      this.weights = new long[hitsLength];
    }
  }

  /**
   * Compute dynamic numeric ranges using weights.
   *
   * @param values an array that contains the values of matching documents
   * @param weights an array that contains the weights of matching documents
   * @param len actual length of values and weights
   * @param totalWeight the sum of weight values
   * @param topN the requested top-n parameter
   * @return A list of DynamicRangeInfo that contains count, relevance, min, max, and centroid
   *     values for each range. The size of dynamic ranges may not be exactly equal to top-N. top-N
   *     is used to compute the equi-weight per bin.
   */
  public static List<DynamicRangeInfo> computeDynamicNumericRanges(
      long[] values, long[] weights, int len, long totalWeight, int topN) {
    assert values.length == weights.length && len <= values.length && len >= 0;
    assert topN >= 0;
    List<DynamicRangeInfo> dynamicRangeResult = new ArrayList<>();
    if (len == 0 || topN == 0) {
      return dynamicRangeResult;
    }

    new InPlaceMergeSorter() {
      @Override
      protected int compare(int index1, int index2) {
        int cmp = Long.compare(values[index1], values[index2]);
        if (cmp == 0) {
          // If the values are equal, sort based on the weights.
          // Any weight order is correct as long as it's deterministic.
          return Long.compare(weights[index1], weights[index2]);
        }
        return cmp;
      }

      @Override
      protected void swap(int index1, int index2) {
        long tmp = values[index1];
        values[index1] = values[index2];
        values[index2] = tmp;
        tmp = weights[index1];
        weights[index1] = weights[index2];
        weights[index2] = tmp;
      }
    }.sort(0, len);

    long accuWeight = 0;
    long valueSum = 0;
    int count = 0;
    int minIdx = 0;

    double rangeWeightTarget = (double) totalWeight / Math.min(topN, len);

    for (int i = 0; i < len; i++) {
      accuWeight += weights[i];
      valueSum += values[i];
      count++;

      if (accuWeight >= rangeWeightTarget) {
        dynamicRangeResult.add(
            new DynamicRangeInfo(
                count, accuWeight, values[minIdx], values[i], (double) valueSum / count));
        count = 0;
        accuWeight = 0;
        valueSum = 0;
        minIdx = i + 1;
      }
    }

    // capture the remaining values to create the last range
    if (minIdx < len) {
      dynamicRangeResult.add(
          new DynamicRangeInfo(
              count, accuWeight, values[minIdx], values[len - 1], (double) valueSum / count));
    }
    return dynamicRangeResult;
  }

  /** Holds parameters of a dynamic numeric range. */
  public static class DynamicRangeInfo {
    /** the number of items in the range */
    public int count;

    /** the summed weight of the items in the range */
    public long weight;

    /** the lower bound of the range (inclusive) */
    public long min;

    /** upper bound of the range (inclusive) */
    public long max;

    /** the average value in the range */
    public double centroid;

    /** All args constructor */
    public DynamicRangeInfo(int count, long weight, long min, long max, double centroid) {
      this.count = count;
      this.weight = weight;
      this.min = min;
      this.max = max;
      this.centroid = centroid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DynamicRangeInfo that = (DynamicRangeInfo) o;

      return (count == that.count)
          && (weight == that.weight)
          && (min == that.min)
          && (max == that.max)
          && (centroid == that.centroid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, weight, min, max, centroid);
    }
  }
}
