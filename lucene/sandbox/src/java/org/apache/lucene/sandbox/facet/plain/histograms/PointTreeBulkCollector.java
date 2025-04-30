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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;

/**
 * Logic for bulk collecting histogram using multi range traversal on PointTree. If current range
 * being collected contains the TreeNode range completely, we can completely skip traversing the
 * TreeNodes individually
 *
 * @lucene.experimental
 */
class PointTreeBulkCollector {
  private static Function<byte[], Long> bytesToLong(int numBytes) {
    if (numBytes == Long.BYTES) {
      // Used by LongPoint, DoublePoint
      return a -> NumericUtils.sortableBytesToLong(a, 0);
    } else if (numBytes == Integer.BYTES) {
      // Used by IntPoint, FloatPoint, LatLonPoint, LatLonShape
      return a -> (long) NumericUtils.sortableBytesToInt(a, 0);
    }

    return null;
  }

  static boolean canCollectEfficiently(final PointValues pointValues, final long bucketWidth)
      throws IOException {
    // We need pointValues.getDocCount() == pointValues.size() to count each doc only
    // once, including docs that have two values that fall into the same bucket.
    if (pointValues == null
        || pointValues.getNumDimensions() != 1
        || pointValues.getDocCount() != pointValues.size()) {
      return false;
    }

    final Function<byte[], Long> byteToLong = bytesToLong(pointValues.getBytesPerDimension());
    if (byteToLong == null) {
      return false;
    }

    long leafMinBucket =
        Math.floorDiv(byteToLong.apply(pointValues.getMinPackedValue()), bucketWidth);
    long leafMaxBucket =
        Math.floorDiv(byteToLong.apply(pointValues.getMaxPackedValue()), bucketWidth);

    // We want that # leaf nodes is more than # buckets so that we can completely skip over
    // some of the leaf nodes. Higher this ratio, more efficient it is than naive approach!
    if ((pointValues.size() / 512) < (leafMaxBucket - leafMinBucket)) {
      return false;
    }

    return true;
  }

  static void collect(
      final PointValues pointValues,
      final long bucketWidth,
      final LongIntHashMap collectorCounts,
      final int maxBuckets)
      throws IOException {
    final Function<byte[], Long> byteToLong = bytesToLong(pointValues.getBytesPerDimension());
    BucketManager collector =
        new BucketManager(
            collectorCounts,
            byteToLong.apply(pointValues.getMinPackedValue()),
            bucketWidth,
            byteToLong,
            maxBuckets);
    PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
    intersectWithRanges(visitor, pointValues.getPointTree(), collector);
    collector.finalizePreviousBucket(null);
  }

  private static void intersectWithRanges(
      PointValues.IntersectVisitor visitor,
      PointValues.PointTree pointTree,
      BucketManager collector)
      throws IOException {
    PointValues.Relation r =
        visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

    switch (r) {
      case CELL_INSIDE_QUERY:
        collector.countNode((int) pointTree.size());
        break;
      case CELL_CROSSES_QUERY:
        if (pointTree.moveToChild()) {
          do {
            intersectWithRanges(visitor, pointTree, collector);
          } while (pointTree.moveToSibling());
          pointTree.moveToParent();
        } else {
          pointTree.visitDocValues(visitor);
        }
        break;
      case CELL_OUTSIDE_QUERY:
    }
  }

  private static PointValues.IntersectVisitor getIntersectVisitor(BucketManager collector) {
    return new PointValues.IntersectVisitor() {
      @Override
      public void visit(int docID) {
        // this branch should be unreachable
        throw new UnsupportedOperationException(
            "This IntersectVisitor does not perform any actions on a "
                + "docID="
                + docID
                + " node being visited");
      }

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        if (!collector.withinUpperBound(packedValue)) {
          collector.finalizePreviousBucket(packedValue);
        }

        if (collector.withinRange(packedValue)) {
          collector.count();
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
        if (!collector.withinUpperBound(packedValue)) {
          collector.finalizePreviousBucket(packedValue);
        }

        if (collector.withinRange(packedValue)) {
          for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
            collector.count();
          }
        }
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        // try to find the first range that may collect values from this cell
        if (!collector.withinUpperBound(minPackedValue)) {
          collector.finalizePreviousBucket(minPackedValue);
        }

        // Not possible to have the CELL_OUTSIDE_QUERY, as bucket lower bound is updated
        // while finalizing the previous bucket
        if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
          return PointValues.Relation.CELL_INSIDE_QUERY;
        }
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
    };
  }

  private static class BucketManager {
    private final LongIntHashMap collectorCounts;
    private int counter = 0;
    private long startValue;
    private long endValue;
    private int nonZeroBuckets = 0;
    private int maxBuckets;
    private Function<byte[], Long> byteToLong;
    private long bucketWidth;

    public BucketManager(
        LongIntHashMap collectorCounts,
        long minValue,
        long bucketWidth,
        Function<byte[], Long> byteToLong,
        int maxBuckets) {
      this.collectorCounts = collectorCounts;
      this.bucketWidth = bucketWidth;
      this.startValue = Math.floorDiv(minValue, bucketWidth) * bucketWidth;
      this.endValue = startValue + bucketWidth;
      this.byteToLong = byteToLong;
      this.maxBuckets = maxBuckets;
    }

    private void count() {
      counter++;
    }

    private void countNode(int count) {
      counter += count;
    }

    private void finalizePreviousBucket(byte[] packedValue) {
      // TODO: Can counter ever be 0?
      if (counter > 0) {
        collectorCounts.addTo(Math.floorDiv(startValue, bucketWidth), counter);
        if (packedValue != null) {
          startValue = byteToLong.apply(packedValue);
          // Align the start value with bucket width
          startValue = Math.floorDiv(startValue, bucketWidth) * bucketWidth;
          endValue = startValue + bucketWidth;
        }
        nonZeroBuckets++;
        counter = 0;
        HistogramCollector.checkMaxBuckets(nonZeroBuckets, maxBuckets);
      }
    }

    private boolean withinLowerBound(byte[] value) {
      return byteToLong.apply(value) >= startValue;
    }

    private boolean withinUpperBound(byte[] value) {
      return byteToLong.apply(value) < endValue;
    }

    private boolean withinRange(byte[] value) {
      return withinLowerBound(value) && withinUpperBound(value);
    }
  }
}
