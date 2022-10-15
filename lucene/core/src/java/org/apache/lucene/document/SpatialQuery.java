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
package org.apache.lucene.document;

import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

/**
 * Base query class for all spatial geometries: {@link LatLonShape}, {@link LatLonPoint} and {@link
 * XYShape}. In order to create a query, use the factory methods on those classes.
 *
 * @lucene.internal
 */
abstract class SpatialQuery extends Query {
  /** field name */
  final String field;
  /**
   * query relation disjoint: {@link QueryRelation#DISJOINT}, intersects: {@link
   * QueryRelation#INTERSECTS}, within: {@link QueryRelation#DISJOINT}, contains: {@link
   * QueryRelation#CONTAINS}
   */
  final QueryRelation queryRelation;

  final Geometry[] geometries;
  final Component2D queryComponent2D;

  protected SpatialQuery(String field, final QueryRelation queryRelation, Geometry... geometries) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (queryRelation == null) {
      throw new IllegalArgumentException("queryRelation must not be null");
    }
    this.field = field;
    this.queryRelation = queryRelation;
    this.geometries = geometries.clone();
    this.queryComponent2D = createComponent2D(geometries);
  }

  protected abstract Component2D createComponent2D(Geometry... geometries);

  /**
   * returns the spatial visitor to be used for this query. Called before generating the query
   * {@link Weight}
   */
  protected abstract SpatialVisitor getSpatialVisitor();

  /** Visitor used for walking the BKD tree. */
  protected abstract static class SpatialVisitor {
    /** relates a range of points (internal node) to the query */
    protected abstract Relation relate(byte[] minPackedValue, byte[] maxPackedValue);

    /** Gets a intersects predicate. Called when constructing a {@link Scorer} */
    protected abstract Predicate<byte[]> intersects();

    /** Gets a within predicate. Called when constructing a {@link Scorer} */
    protected abstract Predicate<byte[]> within();

    /** Gets a contains function. Called when constructing a {@link Scorer} */
    protected abstract Function<byte[], Component2D.WithinRelation> contains();

    private Predicate<byte[]> containsPredicate() {
      final Function<byte[], Component2D.WithinRelation> contains = contains();
      return bytes -> contains.apply(bytes) == Component2D.WithinRelation.CANDIDATE;
    }

    private BiFunction<byte[], byte[], Relation> getInnerFunction(
        ShapeField.QueryRelation queryRelation) {
      if (queryRelation == QueryRelation.DISJOINT) {
        return (minPackedValue, maxPackedValue) ->
            transposeRelation(relate(minPackedValue, maxPackedValue));
      }
      return (minPackedValue, maxPackedValue) -> relate(minPackedValue, maxPackedValue);
    }

    private Predicate<byte[]> getLeafPredicate(ShapeField.QueryRelation queryRelation) {
      switch (queryRelation) {
        case INTERSECTS:
          return intersects();
        case WITHIN:
          return within();
        case DISJOINT:
          return intersects().negate();
        case CONTAINS:
          return containsPredicate();
        default:
          throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
      }
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  protected boolean queryIsCacheable(LeafReaderContext ctx) {
    return true;
  }

  protected ScorerSupplier getScorerSupplier(
      LeafReader reader,
      SpatialVisitor spatialVisitor,
      ScoreMode scoreMode,
      ConstantScoreWeight weight,
      float boost,
      float score)
      throws IOException {
    final PointValues values = reader.getPointValues(field);
    if (values == null) {
      // No docs in this segment had any points fields
      return null;
    }
    final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
    if (fieldInfo == null) {
      // No docs in this segment indexed this field at all
      return null;
    }

    final Relation rel =
        spatialVisitor
            .getInnerFunction(queryRelation)
            .apply(values.getMinPackedValue(), values.getMaxPackedValue());
    if (rel == Relation.CELL_OUTSIDE_QUERY
        || (rel == Relation.CELL_INSIDE_QUERY && queryRelation == QueryRelation.CONTAINS)) {
      // no documents match the query
      return null;
    } else if (values.getDocCount() == reader.maxDoc() && rel == Relation.CELL_INSIDE_QUERY) {
      // all documents match the query
      return new ScorerSupplier() {
        @Override
        public Scorer get(long leadCost) {
          return new ConstantScoreScorer(
              weight, score, scoreMode, DocIdSetIterator.all(reader.maxDoc()));
        }

        @Override
        public long cost() {
          return reader.maxDoc();
        }
      };
    } else {
      if (queryRelation != QueryRelation.INTERSECTS
          && queryRelation != QueryRelation.CONTAINS
          && values.getDocCount() != values.size()
          && hasAnyHits(spatialVisitor, queryRelation, values) == false) {
        // First we check if we have any hits so we are fast in the adversarial case where
        // the shape does not match any documents and we are in the dense case
        return null;
      }
      // walk the tree to get matching documents
      return new RelationScorerSupplier(values, spatialVisitor, queryRelation, field) {
        @Override
        public Scorer get(long leadCost) throws IOException {
          return getScorer(reader, weight, score, scoreMode);
        }
      };
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    final SpatialQuery query = this;
    final SpatialVisitor spatialVisitor = getSpatialVisitor();
    return new ConstantScoreWeight(query, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();
        return getScorerSupplier(reader, spatialVisitor, scoreMode, this, boost, score());
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return queryIsCacheable(ctx);
      }
    };
  }

  /** returns the field name */
  public String getField() {
    return field;
  }

  /** returns the query relation */
  public QueryRelation getQueryRelation() {
    return queryRelation;
  }

  @Override
  public int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + queryRelation.hashCode();
    hash = 31 * hash + Arrays.hashCode(geometries);
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(o);
  }

  /** class specific equals check */
  protected boolean equalsTo(Object o) {
    return Objects.equals(field, ((SpatialQuery) o).field)
        && this.queryRelation == ((SpatialQuery) o).queryRelation
        && Arrays.equals(geometries, ((SpatialQuery) o).geometries);
  }

  /**
   * transpose the relation; INSIDE becomes OUTSIDE, OUTSIDE becomes INSIDE, CROSSES remains
   * unchanged
   */
  protected static Relation transposeRelation(Relation r) {
    if (r == Relation.CELL_INSIDE_QUERY) {
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (r == Relation.CELL_OUTSIDE_QUERY) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /**
   * utility class for implementing constant score logic specific to INTERSECT, WITHIN, and DISJOINT
   */
  private abstract static class RelationScorerSupplier extends ScorerSupplier {
    private final PointValues values;
    private final SpatialVisitor spatialVisitor;
    private final QueryRelation queryRelation;
    private final String field;
    private long cost = -1;

    RelationScorerSupplier(
        final PointValues values,
        SpatialVisitor spatialVisitor,
        final QueryRelation queryRelation,
        final String field) {
      this.values = values;
      this.spatialVisitor = spatialVisitor;
      this.queryRelation = queryRelation;
      this.field = field;
    }

    protected Scorer getScorer(
        final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode)
        throws IOException {
      switch (queryRelation) {
        case INTERSECTS:
          return getSparseScorer(reader, weight, boost, scoreMode);
        case CONTAINS:
          return getContainsDenseScorer(reader, weight, boost, scoreMode);
        case WITHIN:
        case DISJOINT:
          return values.getDocCount() == values.size()
              ? getSparseScorer(reader, weight, boost, scoreMode)
              : getDenseScorer(reader, weight, boost, scoreMode);
        default:
          throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
      }
    }

    /** Scorer used for INTERSECTS and single value points */
    private Scorer getSparseScorer(
        final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode)
        throws IOException {
      if (queryRelation == QueryRelation.DISJOINT
          && values.getDocCount() == reader.maxDoc()
          && values.getDocCount() == values.size()
          && cost() > reader.maxDoc() / 2) {
        // If all docs have exactly one value and the cost is greater
        // than half the leaf size then maybe we can make things faster
        // by computing the set of documents that do NOT match the query
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.set(0, reader.maxDoc());
        final long[] cost = new long[] {reader.maxDoc()};
        values.intersect(getInverseDenseVisitor(spatialVisitor, queryRelation, result, cost));
        final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      } else if (values.getDocCount() < (values.size() >>> 2)) {
        // we use a dense structure so we can skip already visited documents
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        final long[] cost = new long[] {0};
        values.intersect(getIntersectsDenseVisitor(spatialVisitor, queryRelation, result, cost));
        assert cost[0] > 0 || result.cardinality() == 0;
        final DocIdSetIterator iterator =
            cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      } else {
        final DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(reader.maxDoc(), values, field);
        values.intersect(getSparseVisitor(spatialVisitor, queryRelation, docIdSetBuilder));
        final DocIdSetIterator iterator = docIdSetBuilder.build().iterator();
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      }
    }

    /** Scorer used for WITHIN and DISJOINT */
    private Scorer getDenseScorer(
        LeafReader reader, Weight weight, final float boost, ScoreMode scoreMode)
        throws IOException {
      final FixedBitSet result = new FixedBitSet(reader.maxDoc());
      final long[] cost;
      if (values.getDocCount() == reader.maxDoc()) {
        cost = new long[] {values.size()};
        // In this case we can spare one visit to the tree, all documents
        // are potential matches
        result.set(0, reader.maxDoc());
        // Remove false positives
        values.intersect(getInverseDenseVisitor(spatialVisitor, queryRelation, result, cost));
      } else {
        cost = new long[] {0};
        // Get potential  documents.
        final FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
        values.intersect(getDenseVisitor(spatialVisitor, queryRelation, result, excluded, cost));
        result.andNot(excluded);
        // Remove false positives, we only care about the inner nodes as intersecting
        // leaf nodes have been already taken into account. Unfortunately this
        // process still reads the leaf nodes.
        values.intersect(getShallowInverseDenseVisitor(spatialVisitor, queryRelation, result));
      }
      assert cost[0] > 0 || result.cardinality() == 0;
      final DocIdSetIterator iterator =
          cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    private Scorer getContainsDenseScorer(
        LeafReader reader, Weight weight, final float boost, ScoreMode scoreMode)
        throws IOException {
      final FixedBitSet result = new FixedBitSet(reader.maxDoc());
      final long[] cost = new long[] {0};
      // Get potential  documents.
      final FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
      values.intersect(
          getContainsDenseVisitor(spatialVisitor, queryRelation, result, excluded, cost));
      result.andNot(excluded);
      assert cost[0] > 0 || result.cardinality() == 0;
      final DocIdSetIterator iterator =
          cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    @Override
    public long cost() {
      if (cost == -1) {
        // Computing the cost may be expensive, so only do it if necessary
        cost = values.estimateDocCount(getEstimateVisitor(spatialVisitor, queryRelation));
        assert cost >= 0;
      }
      return cost;
    }
  }

  /** create a visitor for calculating point count estimates for the provided relation */
  private static IntersectVisitor getEstimateVisitor(
      final SpatialVisitor spatialVisitor, QueryRelation queryRelation) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void visit(int docID, byte[] t) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a sparse bitset. (Used by
   * INTERSECT when the number of docs <= 4 * number of points )
   */
  private static IntersectVisitor getSparseVisitor(
      final SpatialVisitor spatialVisitor,
      QueryRelation queryRelation,
      final DocIdSetBuilder result) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {
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
      public void visit(int docID, byte[] t) {
        if (leafPredicate.test(t)) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t)) {
          adder.add(iterator);
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /** Scorer used for INTERSECTS when the number of points > 4 * number of docs */
  private static IntersectVisitor getIntersectsDenseVisitor(
      final SpatialVisitor spatialVisitor,
      QueryRelation queryRelation,
      final FixedBitSet result,
      final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {

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
      public void visit(int docID, byte[] t) {
        if (result.get(docID) == false) {
          if (leafPredicate.test(t)) {
            visit(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t)) {
          visit(iterator);
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a dense bitset; used with
   * WITHIN & DISJOINT
   */
  private static IntersectVisitor getDenseVisitor(
      final SpatialVisitor spatialVisitor,
      final QueryRelation queryRelation,
      final FixedBitSet result,
      final FixedBitSet excluded,
      final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {
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
      public void visit(int docID, byte[] t) {
        if (excluded.get(docID) == false) {
          if (leafPredicate.test(t)) {
            visit(docID);
          } else {
            excluded.set(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t)) {
          visit(iterator);
        } else {
          excluded.or(iterator);
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a dense bitset; used with
   * CONTAINS
   */
  private static IntersectVisitor getContainsDenseVisitor(
      final SpatialVisitor spatialVisitor,
      final QueryRelation queryRelation,
      final FixedBitSet result,
      final FixedBitSet excluded,
      final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    final Function<byte[], Component2D.WithinRelation> leafFunction = spatialVisitor.contains();
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        excluded.set(docID);
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        excluded.or(iterator);
      }

      @Override
      public void visit(int docID, byte[] t) {
        if (excluded.get(docID) == false) {
          Component2D.WithinRelation within = leafFunction.apply(t);
          if (within == Component2D.WithinRelation.CANDIDATE) {
            cost[0]++;
            result.set(docID);
          } else if (within == Component2D.WithinRelation.NOTWITHIN) {
            excluded.set(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        Component2D.WithinRelation within = leafFunction.apply(t);
        int docID;
        while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (within == Component2D.WithinRelation.CANDIDATE) {
            cost[0]++;
            result.set(docID);
          } else if (within == Component2D.WithinRelation.NOTWITHIN) {
            excluded.set(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that clears documents that do not match the polygon query using a dense
   * bitset; used with WITHIN & DISJOINT
   */
  private static IntersectVisitor getInverseDenseVisitor(
      final SpatialVisitor spatialVisitor,
      final QueryRelation queryRelation,
      final FixedBitSet result,
      final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.clear(docID);
        cost[0]--;
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        result.andNot(iterator);
        cost[0] = Math.max(0, cost[0] - iterator.cost());
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        if (result.get(docID)) {
          if (leafPredicate.test(packedTriangle) == false) {
            visit(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t) == false) {
          visit(iterator);
        }
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(innerFunction.apply(minPackedValue, maxPackedValue));
      }
    };
  }

  /**
   * create a visitor that clears documents that do not match the polygon query using a dense
   * bitset; used with WITHIN & DISJOINT. This visitor only takes into account inner nodes
   */
  private static IntersectVisitor getShallowInverseDenseVisitor(
      final SpatialVisitor spatialVisitor, QueryRelation queryRelation, final FixedBitSet result) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
        spatialVisitor.getInnerFunction(queryRelation);
    ;
    return new IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.clear(docID);
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        result.andNot(iterator);
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        // NO-OP
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) {
        // NO-OP
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(innerFunction.apply(minPackedValue, maxPackedValue));
      }
    };
  }

  /**
   * Return true if the query matches at least one document. It creates a visitor that terminates as
   * soon as one or more docs are matched.
   */
  private static boolean hasAnyHits(
      final SpatialVisitor spatialVisitor, QueryRelation queryRelation, final PointValues values)
      throws IOException {
    try {
      final BiFunction<byte[], byte[], Relation> innerFunction =
          spatialVisitor.getInnerFunction(queryRelation);
      final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
      values.intersect(
          new IntersectVisitor() {

            @Override
            public void visit(int docID) {
              throw new CollectionTerminatedException();
            }

            @Override
            public void visit(int docID, byte[] t) {
              if (leafPredicate.test(t)) {
                throw new CollectionTerminatedException();
              }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] t) {
              if (leafPredicate.test(t)) {
                throw new CollectionTerminatedException();
              }
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              Relation rel = innerFunction.apply(minPackedValue, maxPackedValue);
              if (rel == Relation.CELL_INSIDE_QUERY) {
                throw new CollectionTerminatedException();
              }
              return rel;
            }
          });
    } catch (
        @SuppressWarnings("unused")
        CollectionTerminatedException e) {
      return true;
    }
    return false;
  }

  /** Holds spatial logic for a bounding box that works in the encoded space */
  public static class EncodedRectangle {
    protected int minX;
    protected int maxX;
    protected int minY;
    protected int maxY;
    protected boolean wrapsCoordinateSystem;

    protected EncodedRectangle(
        int minX, int maxX, int minY, int maxY, boolean wrapsCoordinateSystem) {
      this.minX = minX;
      this.maxX = maxX;
      this.minY = minY;
      this.maxY = maxY;
      this.wrapsCoordinateSystem = wrapsCoordinateSystem;
    }

    protected boolean wrapsCoordinateSystem() {
      return wrapsCoordinateSystem;
    }

    /** Checks if the rectangle contains the provided point */
    boolean contains(int x, int y) {
      if (y < minY || y > maxY) {
        return false;
      }
      if (wrapsCoordinateSystem()) {
        return (x > maxX && x < minX) == false;
      } else {
        return (x > maxX || x < minX) == false;
      }
    }

    /** Checks if the rectangle intersects the provided LINE */
    boolean intersectsLine(int aX, int aY, int bX, int bY) {
      if (contains(aX, aY) || contains(bX, bY)) {
        return true;
      }
      // check bounding boxes are disjoint
      if (StrictMath.max(aY, bY) < minY || StrictMath.min(aY, bY) > maxY) {
        return false;
      }
      if (wrapsCoordinateSystem) { // crosses dateline
        if (StrictMath.min(aX, bX) > maxX && StrictMath.max(aX, bX) < minX) {
          return false;
        }
      } else {
        if (StrictMath.min(aX, bX) > maxX || StrictMath.max(aX, bX) < minX) {
          return false;
        }
      }
      // expensive part
      return edgeIntersectsQuery(aX, aY, bX, bY);
    }

    /** Checks if the rectangle intersects the provided triangle */
    boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      // query contains any triangle points
      if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
        return true;
      }
      // check bounding box of triangle
      int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
      int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);
      // check bounding boxes are disjoint
      if (tMaxY < minY || tMinY > maxY) {
        return false;
      }
      int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
      int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
      if (wrapsCoordinateSystem) { // wraps coordinate system
        if (tMinX > maxX && tMaxX < minX) {
          return false;
        }
      } else {
        if (tMinX > maxX || tMaxX < minX) {
          return false;
        }
      }
      // expensive part
      return Component2D.pointInTriangle(
              tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)
          || edgeIntersectsQuery(aX, aY, bX, bY)
          || edgeIntersectsQuery(bX, bY, cX, cY)
          || edgeIntersectsQuery(cX, cY, aX, aY);
    }

    boolean intersectsRectangle(final int minX, final int maxX, final int minY, final int maxY) {
      // simple Y check
      if (this.minY > maxY || this.maxY < minY) {
        return false;
      }

      if (this.minX <= maxX) {
        // if the triangle's minX is less than the query maxX
        if (wrapsCoordinateSystem || this.maxX >= minX) {
          // intersects if the query box is wrapping (western box) or
          // the triangle maxX is greater than the query minX
          return true;
        }
      }

      return wrapsCoordinateSystem;
    }

    boolean containsRectangle(final int minX, final int maxX, final int minY, final int maxY) {
      return this.minX <= minX && this.maxX >= maxX && this.minY <= minY && this.maxY >= maxY;
    }

    /** Checks if the rectangle contains the provided LINE */
    boolean containsLine(int aX, int aY, int bX, int bY) {
      if (aY < minY || bY < minY || aY > maxY || bY > maxY) {
        return false;
      }
      if (wrapsCoordinateSystem) { // wraps coordinate system
        return (aX >= minX && bX >= minX) || (aX <= maxX && bX <= maxX);
      } else {
        return aX >= minX && bX >= minX && aX <= maxX && bX <= maxX;
      }
    }

    /** Checks if the rectangle contains the provided triangle */
    boolean containsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      if (aY < minY || bY < minY || cY < minY || aY > maxY || bY > maxY || cY > maxY) {
        return false;
      }
      if (wrapsCoordinateSystem) { // wraps coordinate system
        return (aX >= minX && bX >= minX && cX >= minX) || (aX <= maxX && bX <= maxX && cX <= maxX);
      } else {
        return aX >= minX && bX >= minX && cX >= minX && aX <= maxX && bX <= maxX && cX <= maxX;
      }
    }

    /** Returns the Within relation to the provided triangle */
    Component2D.WithinRelation withinLine(int aX, int aY, boolean ab, int bX, int bY) {
      if (contains(aX, aY) || contains(bX, bY)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }
      if (ab == true && edgeIntersectsBox(aX, aY, bX, bY, minX, maxX, minY, maxY) == true) {
        return Component2D.WithinRelation.NOTWITHIN;
      }
      return Component2D.WithinRelation.DISJOINT;
    }

    /** Returns the Within relation to the provided triangle */
    Component2D.WithinRelation withinTriangle(
        int aX, int aY, boolean ab, int bX, int bY, boolean bc, int cX, int cY, boolean ca) {
      // Points belong to the shape so if points are inside the rectangle then it cannot be within.
      if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }

      // Bounding boxes disjoint?
      int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
      int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);
      // check bounding boxes are disjoint
      if (tMaxY < minY || tMinY > maxY) {
        return Component2D.WithinRelation.DISJOINT;
      }
      int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
      int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
      if (wrapsCoordinateSystem) { // wraps coordinate system
        if (tMinX > maxX && tMaxX < minX) {
          return Component2D.WithinRelation.DISJOINT;
        }
      } else {
        if (tMinX > maxX || tMaxX < minX) {
          return Component2D.WithinRelation.DISJOINT;
        }
      }
      // If any of the edges intersects an edge belonging to the shape then it cannot be within.
      Component2D.WithinRelation relation = Component2D.WithinRelation.DISJOINT;
      if (edgeIntersectsBox(aX, aY, bX, bY, minX, maxX, minY, maxY) == true) {
        if (ab == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      if (edgeIntersectsBox(bX, bY, cX, cY, minX, maxX, minY, maxY) == true) {
        if (bc == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }

      if (edgeIntersectsBox(cX, cY, aX, aY, minX, maxX, minY, maxY) == true) {
        if (ca == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      // Check if shape is within the triangle
      if (relation == Component2D.WithinRelation.CANDIDATE
          || Component2D.pointInTriangle(
              tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)) {
        return Component2D.WithinRelation.CANDIDATE;
      }
      return relation;
    }

    /** returns true if the edge (defined by (aX, aY) (bX, bY)) intersects the query */
    private boolean edgeIntersectsQuery(int aX, int aY, int bX, int bY) {
      if (wrapsCoordinateSystem) {
        return edgeIntersectsBox(aX, aY, bX, bY, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
            || edgeIntersectsBox(aX, aY, bX, bY, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
      }
      return edgeIntersectsBox(aX, aY, bX, bY, this.minX, this.maxX, this.minY, this.maxY);
    }

    /** returns true if the edge (defined by (aX, aY) (bX, bY)) intersects the box */
    private static boolean edgeIntersectsBox(
        int aX, int aY, int bX, int bY, int minX, int maxX, int minY, int maxY) {
      if (Math.max(aX, bX) < minX
          || Math.min(aX, bX) > maxX
          || Math.min(aY, bY) > maxY
          || Math.max(aY, bY) < minY) {
        return false;
      }
      return GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, maxY, maxX, maxY)
          || // top
          GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, maxY, maxX, minY)
          || // bottom
          GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, minY, minX, minY)
          || // left
          GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, minY, minX, maxY); // right
    }
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("[");
    for (int i = 0; i < geometries.length; i++) {
      sb.append(geometries[i].toString());
      sb.append(',');
    }
    sb.append(']');
    return sb.toString();
  }
}
