package org.apache.lucene.sandbox.facet.plain.histograms;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;

class PointTreeTraversal {
  static void multiRangesTraverse(
      final PointValues.PointTree tree, final Ranges ranges, final LongIntHashMap collectorCounts)
      throws IOException {
    int activeIndex = ranges.firstRangeIndex(tree.getMinPackedValue(), tree.getMaxPackedValue());

    // No ranges match the query, skip the collection completely
    if (activeIndex < 0) {
      return;
    }
    RangeCollectorForPointTree collector =
        new RangeCollectorForPointTree(collectorCounts, ranges, activeIndex);
    PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
    try {
      intersectWithRanges(visitor, tree, collector);
    } catch (CollectionTerminatedException _) {
      // Early terminate since no more range to collect
    }
    collector.finalizePreviousRange();
  }

  static Ranges buildRanges(long bucketWidth) {
    // TODO: Add logic for building ranges
    return new Ranges(new byte[1][1], new byte[1][1]);
  }

  private static void intersectWithRanges(
      PointValues.IntersectVisitor visitor,
      PointValues.PointTree pointTree,
      RangeCollectorForPointTree collector)
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

  private static PointValues.IntersectVisitor getIntersectVisitor(
      RangeCollectorForPointTree collector) {
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
        visitPoints(packedValue, collector::count);
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
        visitPoints(
            packedValue,
            () -> {
              try {
                for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                  collector.count();
                }
              } catch (Exception _) {
              }
            });
      }

      private void visitPoints(byte[] packedValue, Runnable collect) {
        if (!collector.withinUpperBound(packedValue)) {
          collector.finalizePreviousRange();
          if (collector.iterateRangeEnd(packedValue)) {
            throw new CollectionTerminatedException();
          }
        }

        if (collector.withinRange(packedValue)) {
          collect.run();
        }
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        // try to find the first range that may collect values from this cell
        if (!collector.withinUpperBound(minPackedValue)) {
          collector.finalizePreviousRange();
          if (collector.iterateRangeEnd(minPackedValue)) {
            throw new CollectionTerminatedException();
          }
        }
        // after the loop, min < upper
        // cell could be outside [min max] lower
        if (!collector.withinLowerBound(maxPackedValue)) {
          return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
          return PointValues.Relation.CELL_INSIDE_QUERY;
        }
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
    };
  }

  private static class RangeCollectorForPointTree {
    private final LongIntHashMap collectorCounts;
    private int counter = 0;
    private final Ranges ranges;
    private int activeIndex;

    public RangeCollectorForPointTree(
        LongIntHashMap collectorCounts, Ranges ranges, int activeIndex) {
      this.collectorCounts = collectorCounts;
      this.ranges = ranges;
      this.activeIndex = activeIndex;
    }

    private void count() {
      counter++;
    }

    private void countNode(int count) {
      counter += count;
    }

    private void finalizePreviousRange() {
      if (counter > 0) {
        collectorCounts.addTo(activeIndex, counter);
        counter = 0;
      }
    }

    /**
     * @return true when iterator exhausted
     */
    private boolean iterateRangeEnd(byte[] value) {
      // the new value may not be contiguous to the previous one
      // so try to find the first next range that cross the new value
      while (!withinUpperBound(value)) {
        if (++activeIndex >= ranges.size) {
          return true;
        }
      }
      return false;
    }

    private boolean withinLowerBound(byte[] value) {
      return Ranges.withinLowerBound(value, ranges.lowers[activeIndex]);
    }

    private boolean withinUpperBound(byte[] value) {
      return Ranges.withinUpperBound(value, ranges.uppers[activeIndex]);
    }

    private boolean withinRange(byte[] value) {
      return withinLowerBound(value) && withinUpperBound(value);
    }
  }

  static class Ranges {
    byte[][] lowers; // inclusive
    byte[][] uppers; // exclusive
    int size;
    int byteLen;
    static ArrayUtil.ByteArrayComparator comparator;

    Ranges(byte[][] lowers, byte[][] uppers) {
      this.lowers = lowers;
      this.uppers = uppers;
      assert lowers.length == uppers.length;
      this.size = lowers.length;
      this.byteLen = lowers[0].length;
      comparator = ArrayUtil.getUnsignedComparator(byteLen);
    }

    public int firstRangeIndex(byte[] globalMin, byte[] globalMax) {
      if (compareByteValue(lowers[0], globalMax) > 0) {
        return -1;
      }
      int i = 0;
      while (compareByteValue(uppers[i], globalMin) <= 0) {
        i++;
        if (i >= size) {
          return -1;
        }
      }
      return i;
    }

    public static int compareByteValue(byte[] value1, byte[] value2) {
      return comparator.compare(value1, 0, value2, 0);
    }

    public static boolean withinLowerBound(byte[] value, byte[] lowerBound) {
      return compareByteValue(value, lowerBound) >= 0;
    }

    public static boolean withinUpperBound(byte[] value, byte[] upperBound) {
      return compareByteValue(value, upperBound) < 0;
    }
  }
}
