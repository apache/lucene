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
package org.apache.lucene.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetAddress;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.bkd.BKDConfig;

/**
 * Access to indexed numeric values.
 *
 * <p>Points represent numeric values and are indexed differently than ordinary text. Instead of an
 * inverted index, points are indexed with datastructures such as <a
 * href="https://en.wikipedia.org/wiki/K-d_tree">KD-trees</a>. These structures are optimized for
 * operations such as <i>range</i>, <i>distance</i>, <i>nearest-neighbor</i>, and
 * <i>point-in-polygon</i> queries.
 *
 * <h2>Basic Point Types</h2>
 *
 * <table>
 *   <caption>Basic point types in Java and Lucene</caption>
 *   <tr><th>Java type</th><th>Lucene class</th></tr>
 *   <tr><td>{@code int}</td><td>{@link IntPoint}</td></tr>
 *   <tr><td>{@code long}</td><td>{@link LongPoint}</td></tr>
 *   <tr><td>{@code float}</td><td>{@link FloatPoint}</td></tr>
 *   <tr><td>{@code double}</td><td>{@link DoublePoint}</td></tr>
 *   <tr><td>{@code byte[]}</td><td>{@link BinaryPoint}</td></tr>
 *   <tr><td>{@link InetAddress}</td><td>{@link InetAddressPoint}</td></tr>
 *   <tr><td>{@link BigInteger}</td><td><a href="{@docRoot}/../sandbox/org/apache/lucene/sandbox/document/BigIntegerPoint.html">BigIntegerPoint</a>*</td></tr>
 * </table>
 *
 * * in the <i>lucene-sandbox</i> jar<br>
 *
 * <p>Basic Lucene point types behave like their java peers: for example {@link IntPoint} represents
 * a signed 32-bit {@link Integer}, supporting values ranging from {@link Integer#MIN_VALUE} to
 * {@link Integer#MAX_VALUE}, ordered consistent with {@link Integer#compareTo(Integer)}. In
 * addition to indexing support, point classes also contain static methods (such as {@link
 * IntPoint#newRangeQuery(String, int, int)}) for creating common queries. For example:
 *
 * <pre class="prettyprint">
 *   // add year 1970 to document
 *   document.add(new IntPoint("year", 1970));
 *   // index document
 *   writer.addDocument(document);
 *   ...
 *   // issue range query of 1960-1980
 *   Query query = IntPoint.newRangeQuery("year", 1960, 1980);
 *   TopDocs docs = searcher.search(query, ...);
 * </pre>
 *
 * <h2>Geospatial Point Types</h2>
 *
 * Although basic point types such as {@link DoublePoint} support points in multi-dimensional space
 * too, Lucene has specialized classes for location data. These classes are optimized for location
 * data: they are more space-efficient and support special operations such as <i>distance</i> and
 * <i>polygon</i> queries. There are currently two implementations: <br>
 *
 * <ol>
 *   <li>{@link LatLonPoint}: indexes {@code (latitude,longitude)} as {@code (x,y)} in
 *       two-dimensional space.
 *   <li><a
 *       href="{@docRoot}/../spatial3d/org/apache/lucene/spatial3d/Geo3DPoint.html">Geo3DPoint</a>*
 *       in <i>lucene-spatial3d</i>: indexes {@code (latitude,longitude)} as {@code (x,y,z)} in
 *       three-dimensional space.
 * </ol>
 *
 * * does <b>not</b> support altitude, 3D here means "uses three dimensions under-the-hood"<br>
 *
 * <h2>Advanced usage</h2>
 *
 * Custom structures can be created on top of single- or multi- dimensional basic types, on top of
 * {@link BinaryPoint} for more flexibility, or via custom {@link Field} subclasses.
 *
 * @lucene.experimental
 */
public abstract class PointValues {

  /** Maximum number of bytes for each dimension */
  public static final int MAX_NUM_BYTES = 16;

  /** Maximum number of dimensions */
  public static final int MAX_DIMENSIONS = BKDConfig.MAX_DIMS;

  /** Maximum number of index dimensions */
  public static final int MAX_INDEX_DIMENSIONS = BKDConfig.MAX_INDEX_DIMS;

  /**
   * Return the cumulated number of points across all leaves of the given {@link IndexReader}.
   * Leaves that do not have points for the given field are ignored.
   *
   * @see PointValues#size()
   */
  public static long size(IndexReader reader, String field) throws IOException {
    long size = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      PointValues values = ctx.reader().getPointValues(field);
      if (values != null) {
        size += values.size();
      }
    }
    return size;
  }

  /**
   * Return the cumulated number of docs that have points across all leaves of the given {@link
   * IndexReader}. Leaves that do not have points for the given field are ignored.
   *
   * @see PointValues#getDocCount()
   */
  public static int getDocCount(IndexReader reader, String field) throws IOException {
    int count = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      PointValues values = ctx.reader().getPointValues(field);
      if (values != null) {
        count += values.getDocCount();
      }
    }
    return count;
  }

  /**
   * Return the minimum packed values across all leaves of the given {@link IndexReader}. Leaves
   * that do not have points for the given field are ignored.
   *
   * @see PointValues#getMinPackedValue()
   */
  public static byte[] getMinPackedValue(IndexReader reader, String field) throws IOException {
    byte[] minValue = null;
    for (LeafReaderContext ctx : reader.leaves()) {
      PointValues values = ctx.reader().getPointValues(field);
      if (values == null) {
        continue;
      }
      byte[] leafMinValue = values.getMinPackedValue();
      if (leafMinValue == null) {
        continue;
      }
      if (minValue == null) {
        minValue = leafMinValue.clone();
      } else {
        final int numDimensions = values.getNumIndexDimensions();
        final int numBytesPerDimension = values.getBytesPerDimension();
        final ByteArrayComparator comparator =
            ArrayUtil.getUnsignedComparator(numBytesPerDimension);
        for (int i = 0; i < numDimensions; ++i) {
          int offset = i * numBytesPerDimension;
          if (comparator.compare(leafMinValue, offset, minValue, offset) < 0) {
            System.arraycopy(leafMinValue, offset, minValue, offset, numBytesPerDimension);
          }
        }
      }
    }
    return minValue;
  }

  /**
   * Return the maximum packed values across all leaves of the given {@link IndexReader}. Leaves
   * that do not have points for the given field are ignored.
   *
   * @see PointValues#getMaxPackedValue()
   */
  public static byte[] getMaxPackedValue(IndexReader reader, String field) throws IOException {
    byte[] maxValue = null;
    for (LeafReaderContext ctx : reader.leaves()) {
      PointValues values = ctx.reader().getPointValues(field);
      if (values == null) {
        continue;
      }
      byte[] leafMaxValue = values.getMaxPackedValue();
      if (leafMaxValue == null) {
        continue;
      }
      if (maxValue == null) {
        maxValue = leafMaxValue.clone();
      } else {
        final int numDimensions = values.getNumIndexDimensions();
        final int numBytesPerDimension = values.getBytesPerDimension();
        final ByteArrayComparator comparator =
            ArrayUtil.getUnsignedComparator(numBytesPerDimension);
        for (int i = 0; i < numDimensions; ++i) {
          int offset = i * numBytesPerDimension;
          if (comparator.compare(leafMaxValue, offset, maxValue, offset) > 0) {
            System.arraycopy(leafMaxValue, offset, maxValue, offset, numBytesPerDimension);
          }
        }
      }
    }
    return maxValue;
  }

  /** Default constructor */
  protected PointValues() {}

  /** Used by {@link #intersect} to check how each recursive cell corresponds to the query. */
  public enum Relation {
    /** Return this if the cell is fully contained by the query */
    CELL_INSIDE_QUERY,
    /** Return this if the cell and query do not overlap */
    CELL_OUTSIDE_QUERY,
    /** Return this if the cell partially overlaps the query */
    CELL_CROSSES_QUERY
  };

  /** Create a new {@link PointTree} to navigate the index */
  public abstract PointTree getPointTree() throws IOException;

  /**
   * Basic operations to read the KD-tree.
   *
   * @lucene.experimental
   */
  public interface PointTree extends Cloneable {

    /** Clone, the current node becomes the root of the new tree. */
    PointTree clone();

    /**
     * Move to the first child node and return {@code true} upon success. Returns {@code false} for
     * leaf nodes and {@code true} otherwise.
     */
    boolean moveToChild() throws IOException;

    /**
     * Move to the next sibling node and return {@code true} upon success. Returns {@code false} if
     * the current node has no more siblings.
     */
    boolean moveToSibling() throws IOException;

    /**
     * Move to the parent node and return {@code true} upon success. Returns {@code false} for the
     * root node and {@code true} otherwise.
     */
    boolean moveToParent() throws IOException;

    /** Return the minimum packed value of the current node. */
    byte[] getMinPackedValue();

    /** Return the maximum packed value of the current node. */
    byte[] getMaxPackedValue();

    /** Return the number of points below the current node. */
    long size();

    /** Visit all the docs below the current node. */
    void visitDocIDs(IntersectVisitor visitor) throws IOException;

    /** Visit all the docs and values below the current node. */
    void visitDocValues(IntersectVisitor visitor) throws IOException;
  }

  /**
   * We recurse the {@link PointTree}, using a provided instance of this to guide the recursion.
   *
   * @lucene.experimental
   */
  public interface IntersectVisitor {
    /**
     * Called for all documents in a leaf cell that's fully contained by the query. The consumer
     * should blindly accept the docID.
     */
    void visit(int docID) throws IOException;

    /**
     * Similar to {@link IntersectVisitor#visit(int)}, but a bulk visit and implements may have
     * their optimizations.
     */
    default void visit(DocIdSetIterator iterator) throws IOException {
      int docID;
      while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        visit(docID);
      }
    }

    /**
     * Called for all documents in a leaf cell that crosses the query. The consumer should
     * scrutinize the packedValue to decide whether to accept it. In the 1D case, values are visited
     * in increasing order, and in the case of ties, in increasing docID order.
     */
    void visit(int docID, byte[] packedValue) throws IOException;

    /**
     * Similar to {@link IntersectVisitor#visit(int, byte[])} but in this case the packedValue can
     * have more than one docID associated to it. The provided iterator should not escape the scope
     * of this method so that implementations of PointValues are free to reuse it,
     */
    default void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
      int docID;
      while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        visit(docID, packedValue);
      }
    }

    /**
     * Called for non-leaf cells to test how the cell relates to the query, to determine how to
     * further recurse down the tree.
     */
    Relation compare(byte[] minPackedValue, byte[] maxPackedValue);

    /** Notifies the caller that this many documents are about to be visited */
    default void grow(int count) {}
    ;
  }

  /**
   * Finds all documents and points matching the provided visitor. This method does not enforce live
   * documents, so it's up to the caller to test whether each document is deleted, if necessary.
   */
  public final void intersect(IntersectVisitor visitor) throws IOException {
    final PointTree pointTree = getPointTree();
    intersect(visitor, pointTree);
    assert pointTree.moveToParent() == false;
  }

  private void intersect(IntersectVisitor visitor, PointTree pointTree) throws IOException {
    Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
    switch (r) {
      case CELL_OUTSIDE_QUERY:
        // This cell is fully outside the query shape: stop recursing
        break;
      case CELL_INSIDE_QUERY:
        // This cell is fully inside the query shape: recursively add all points in this cell
        // without filtering
        pointTree.visitDocIDs(visitor);
        break;
      case CELL_CROSSES_QUERY:
        // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
        // through and do full filtering:
        if (pointTree.moveToChild()) {
          do {
            intersect(visitor, pointTree);
          } while (pointTree.moveToSibling());
          pointTree.moveToParent();
        } else {
          // TODO: we can assert that the first value here in fact matches what the pointTree
          // claimed?
          // Leaf node; scan and filter all points in this block:
          pointTree.visitDocValues(visitor);
        }
        break;
      default:
        throw new IllegalArgumentException("Unreachable code");
    }
  }

  /**
   * Estimate the number of points that would be visited by {@link #intersect} with the given {@link
   * IntersectVisitor}. This should run many times faster than {@link #intersect(IntersectVisitor)}.
   */
  public final long estimatePointCount(IntersectVisitor visitor) {
    try {
      final PointTree pointTree = getPointTree();
      final long count = estimatePointCount(visitor, pointTree);
      assert pointTree.moveToParent() == false;
      return count;
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private long estimatePointCount(IntersectVisitor visitor, PointTree pointTree)
      throws IOException {
    Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
    switch (r) {
      case CELL_OUTSIDE_QUERY:
        // This cell is fully outside the query shape: no points added
        return 0L;
      case CELL_INSIDE_QUERY:
        // This cell is fully inside the query shape: add all points
        return pointTree.size();
      case CELL_CROSSES_QUERY:
        // The cell crosses the shape boundary: keep recursing
        if (pointTree.moveToChild()) {
          long cost = 0;
          do {
            cost += estimatePointCount(visitor, pointTree);
          } while (pointTree.moveToSibling());
          pointTree.moveToParent();
          return cost;
        } else {
          // Assume half the points matched
          return (pointTree.size() + 1) / 2;
        }
      default:
        throw new IllegalArgumentException("Unreachable code");
    }
  }

  /**
   * Estimate the number of documents that would be matched by {@link #intersect} with the given
   * {@link IntersectVisitor}. This should run many times faster than {@link
   * #intersect(IntersectVisitor)}.
   *
   * @see DocIdSetIterator#cost
   */
  public final long estimateDocCount(IntersectVisitor visitor) {
    long estimatedPointCount = estimatePointCount(visitor);
    int docCount = getDocCount();
    double size = size();
    if (estimatedPointCount >= size) {
      // math all docs
      return docCount;
    } else if (size == docCount || estimatedPointCount == 0L) {
      // if the point count estimate is 0 or we have only single values
      // return this estimate
      return estimatedPointCount;
    } else {
      // in case of multi values estimate the number of docs using the solution provided in
      // https://math.stackexchange.com/questions/1175295/urn-problem-probability-of-drawing-balls-of-k-unique-colors
      // then approximate the solution for points per doc << size() which results in the expression
      // D * (1 - ((N - n) / N)^(N/D))
      // where D is the total number of docs, N the total number of points and n the estimated point
      // count
      long docEstimate =
          (long) (docCount * (1d - Math.pow((size - estimatedPointCount) / size, size / docCount)));
      return docEstimate == 0L ? 1L : docEstimate;
    }
  }

  /**
   * Returns minimum value for each dimension, packed, or null if {@link #size} is <code>0</code>
   */
  public abstract byte[] getMinPackedValue() throws IOException;

  /**
   * Returns maximum value for each dimension, packed, or null if {@link #size} is <code>0</code>
   */
  public abstract byte[] getMaxPackedValue() throws IOException;

  /** Returns how many dimensions are represented in the values */
  public abstract int getNumDimensions() throws IOException;

  /** Returns how many dimensions are used for the index */
  public abstract int getNumIndexDimensions() throws IOException;

  /** Returns the number of bytes per dimension */
  public abstract int getBytesPerDimension() throws IOException;

  /** Returns the total number of indexed points across all documents. */
  public abstract long size();

  /** Returns the total number of documents that have indexed at least one point. */
  public abstract int getDocCount();
}
