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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.search.PointQueryUtils;

/**
 * {@code PointRangeWeight} implements scoring and matching logic for {@link PointRangeQuery}. It
 * efficiently matches documents whose point values fall within a specified multi-dimensional range.
 *
 * <p>This class uses Lucene's point values infrastructure to perform fast range queries, leveraging
 * tree traversal and custom visitors for intersection and counting. It supports both single- and
 * multi-dimensional points, and optimizes for cases where all documents match or where all
 * documents have exactly one value.
 */
public class PointRangeWeight extends ConstantScoreWeight {
  private final String field;
  private final int numDims;
  private final int bytesPerDim;
  private final byte[] lowerPoint;
  private final byte[] upperPoint;
  private final ScoreMode scoreMode;
  private final ArrayUtil.ByteArrayComparator comparator;

  protected PointRangeWeight(PointRangeQuery query, float score, ScoreMode scoreMode) {
    super(query, score);
    this.field = query.field;
    this.numDims = query.numDims;
    this.bytesPerDim = query.bytesPerDim;
    this.lowerPoint = query.lowerPoint;
    this.upperPoint = query.upperPoint;
    this.comparator = query.comparator;
    this.scoreMode = scoreMode;
  }

  private boolean matches(byte[] packedValue) {
    int offset = 0;
    for (int dim = 0; dim < numDims; dim++, offset += bytesPerDim) {
      if (comparator.compare(packedValue, offset, lowerPoint, offset) < 0) {
        // Doc's value is too low, in this dimension
        return false;
      }
      if (comparator.compare(packedValue, offset, upperPoint, offset) > 0) {
        // Doc's value is too high, in this dimension
        return false;
      }
    }
    return true;
  }

  private PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
    return new PointValues.IntersectVisitor() {

      DocIdSetBuilder.BulkAdder adder;

      @Override
      public void grow(int count) {
        adder = result.grow(count);
      }

      @Override
      public void visit(int docID) {
        adder.add(docID);
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        adder.add(iterator);
      }

      @Override
      public void visit(IntsRef ref) {
        adder.add(ref);
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        if (matches(packedValue)) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
        if (matches(packedValue)) {
          adder.add(iterator);
        }
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return PointQueryUtils.relate(
            minPackedValue,
            maxPackedValue,
            lowerPoint,
            upperPoint,
            numDims,
            bytesPerDim,
            comparator);
      }
    };
  }

  /** Create a visitor that sets documents that do NOT match the range. */
  private PointValues.IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, long[] cost) {
    return new PointValues.IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.set(docID);
        cost[0]++;
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        result.or(iterator);
        cost[0] += iterator.cost();
      }

      @Override
      public void visit(IntsRef ref) {
        for (int i = ref.offset, to = ref.offset + ref.length; i < to; i++) {
          result.set(ref.ints[i]);
        }
        cost[0] += ref.length;
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        if (matches(packedValue) == false) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
        if (matches(packedValue) == false) {
          visit(iterator);
        }
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        PointValues.Relation relation =
            PointQueryUtils.relate(
                minPackedValue,
                maxPackedValue,
                lowerPoint,
                upperPoint,
                numDims,
                bytesPerDim,
                comparator);
        switch (relation) {
          case CELL_INSIDE_QUERY:
            // all points match, skip this subtree
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
          case CELL_OUTSIDE_QUERY:
            // none of the points match, clear all documents
            return PointValues.Relation.CELL_INSIDE_QUERY;
          case CELL_CROSSES_QUERY:
          default:
            return relation;
        }
      }
    };
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();

    PointValues values = reader.getPointValues(field);
    if (!PointQueryUtils.checkValidPointValues(values, field, numDims, bytesPerDim)) {
      return null;
    }

    if (values.getDocCount() == 0) {
      return null;
    } else {
      final byte[] fieldPackedLower = values.getMinPackedValue();
      final byte[] fieldPackedUpper = values.getMaxPackedValue();
      for (int i = 0; i < numDims; ++i) {
        int offset = i * bytesPerDim;
        if (comparator.compare(lowerPoint, offset, fieldPackedUpper, offset) > 0
            || comparator.compare(upperPoint, offset, fieldPackedLower, offset) < 0) {
          // If this query is a required clause of a boolean query, then returning null here
          // will help make sure that we don't call ScorerSupplier#get on other required clauses
          // of the same boolean query, which is an expensive operation for some queries (e.g.
          // multi-term queries).
          return null;
        }
      }
    }

    boolean allDocsMatch;
    if (values.getDocCount() == reader.maxDoc()) {
      final byte[] fieldPackedLower = values.getMinPackedValue();
      final byte[] fieldPackedUpper = values.getMaxPackedValue();
      allDocsMatch = true;
      for (int i = 0; i < numDims; ++i) {
        int offset = i * bytesPerDim;
        if (comparator.compare(lowerPoint, offset, fieldPackedLower, offset) > 0
            || comparator.compare(upperPoint, offset, fieldPackedUpper, offset) < 0) {
          allDocsMatch = false;
          break;
        }
      }
    } else {
      allDocsMatch = false;
    }

    if (allDocsMatch) {
      // all docs have a value and all points are within bounds, so everything matches
      return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, reader.maxDoc());
    } else {
      return new ConstantScoreScorerSupplier(score(), scoreMode, reader.maxDoc()) {

        final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
        final PointValues.IntersectVisitor visitor = getIntersectVisitor(result);
        long cost = -1;

        @Override
        public DocIdSetIterator iterator(long leadCost) throws IOException {
          if (values.getDocCount() == reader.maxDoc()
              && values.getDocCount() == values.size()
              && cost() > reader.maxDoc() / 2) {
            // If all docs have exactly one value and the cost is greater
            // than half the leaf size then maybe we can make things faster
            // by computing the set of documents that do NOT match the range
            final FixedBitSet result = new FixedBitSet(reader.maxDoc());
            long[] cost = new long[1];
            values.intersect(getInverseIntersectVisitor(result, cost));
            // Flip the bit set and cost
            result.flip(0, reader.maxDoc());
            cost[0] = Math.max(0, reader.maxDoc() - cost[0]);
            return new BitSetIterator(result, cost[0]);
          }

          values.intersect(visitor);
          return result.build().iterator();
        }

        @Override
        public long cost() {
          if (cost == -1) {
            // Computing the cost may be expensive, so only do it if necessary
            cost = values.estimateDocCount(visitor);
            assert cost >= 0;
          }
          return cost;
        }
      };
    }
  }

  @Override
  public int count(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();

    PointValues values = reader.getPointValues(field);
    if (!PointQueryUtils.checkValidPointValues(values, field, numDims, bytesPerDim)) {
      return 0;
    }

    if (reader.hasDeletions() == false) {
      if (PointQueryUtils.relate(
              values.getMinPackedValue(),
              values.getMaxPackedValue(),
              lowerPoint,
              upperPoint,
              numDims,
              bytesPerDim,
              comparator)
          == PointValues.Relation.CELL_INSIDE_QUERY) {
        return values.getDocCount();
      }
      // only 1D: we have the guarantee that it will actually run fast since there are at most 2
      // crossing leaves.
      // docCount == size : counting according number of points in leaf node, so must be
      // single-valued.
      if (numDims == 1 && values.getDocCount() == values.size()) {
        return (int)
            pointCount(
                values.getPointTree(),
                (minPackedValue, maxPackedValue) ->
                    PointQueryUtils.relate(
                        minPackedValue,
                        maxPackedValue,
                        lowerPoint,
                        upperPoint,
                        numDims,
                        bytesPerDim,
                        comparator),
                this::matches);
      }
    }
    return super.count(context);
  }

  /**
   * Finds the number of points matching the provided range conditions. Using this method is faster
   * than calling {@link PointValues#intersect(PointValues.IntersectVisitor)} to get the count of
   * intersecting points. This method does not enforce live documents, therefore it should only be
   * used when there are no deleted documents.
   *
   * @param pointTree start node of the count operation
   * @param nodeComparator comparator to be used for checking whether the internal node is inside
   *     the range
   * @param leafComparator comparator to be used for checking whether the leaf node is inside the
   *     range
   * @return count of points that match the range
   */
  private long pointCount(
      PointValues.PointTree pointTree,
      BiFunction<byte[], byte[], PointValues.Relation> nodeComparator,
      Predicate<byte[]> leafComparator)
      throws IOException {
    final long[] matchingNodeCount = {0};
    // create a custom IntersectVisitor that records the number of leafNodes that matched
    final PointValues.IntersectVisitor visitor =
        new PointValues.IntersectVisitor() {
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
          public void visit(int docID, byte[] packedValue) {
            if (leafComparator.test(packedValue)) {
              matchingNodeCount[0]++;
            }
          }

          @Override
          public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return nodeComparator.apply(minPackedValue, maxPackedValue);
          }
        };
    pointCount(visitor, pointTree, matchingNodeCount);
    return matchingNodeCount[0];
  }

  private void pointCount(
      PointValues.IntersectVisitor visitor,
      PointValues.PointTree pointTree,
      long[] matchingNodeCount)
      throws IOException {
    PointValues.Relation r =
        visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
    switch (r) {
      case CELL_OUTSIDE_QUERY:
        // This cell is fully outside the query shape: return 0 as the count of its nodes
        return;
      case CELL_INSIDE_QUERY:
        // This cell is fully inside the query shape: return the size of the entire node as the
        // count
        matchingNodeCount[0] += pointTree.size();
        return;
      case CELL_CROSSES_QUERY:
        /*
        The cell crosses the shape boundary, or the cell fully contains the query, so we fall
        through and do full counting.
        */
        if (pointTree.moveToChild()) {
          do {
            pointCount(visitor, pointTree, matchingNodeCount);
          } while (pointTree.moveToSibling());
          pointTree.moveToParent();
        } else {
          // we have reached a leaf node here.
          pointTree.visitDocValues(visitor);
          // leaf node count is saved in the matchingNodeCount array by the visitor
        }
        return;
      default:
        throw new IllegalArgumentException("Unreachable code");
    }
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
