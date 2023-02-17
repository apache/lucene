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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * Finds all previously indexed geo points that comply the given {@link ShapeField.QueryRelation}
 * with the specified array of {@link LatLonGeometry}.
 *
 * <p>The field must be indexed using {@link LatLonDocValuesField} added per document.
 */
class LatLonDocValuesQuery extends Query {

  private final String field;
  private final LatLonGeometry[] geometries;
  private final ShapeField.QueryRelation queryRelation;

  LatLonDocValuesQuery(
      String field, ShapeField.QueryRelation queryRelation, LatLonGeometry... geometries) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (queryRelation == null) {
      throw new IllegalArgumentException("queryRelation must not be null");
    }
    if (queryRelation == ShapeField.QueryRelation.WITHIN) {
      for (LatLonGeometry geometry : geometries) {
        if (geometry instanceof Line) {
          // TODO: line queries do not support within relations
          throw new IllegalArgumentException(
              "LatLonDocValuesPointQuery does not support "
                  + ShapeField.QueryRelation.WITHIN
                  + " queries with line geometries");
        }
      }
    }
    if (queryRelation == ShapeField.QueryRelation.CONTAINS) {
      for (LatLonGeometry geometry : geometries) {
        if ((geometry instanceof Point) == false) {
          throw new IllegalArgumentException(
              "LatLonDocValuesPointQuery does not support "
                  + ShapeField.QueryRelation.CONTAINS
                  + " queries with non-points geometries");
        }
      }
    }
    this.field = field;
    this.geometries = geometries;
    this.queryRelation = queryRelation;
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (!this.field.equals(field)) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(queryRelation).append(':');
    sb.append("geometries(").append(Arrays.toString(geometries));
    return sb.append(")").toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    LatLonDocValuesQuery other = (LatLonDocValuesQuery) obj;
    return field.equals(other.field)
        && queryRelation == other.queryRelation
        && Arrays.equals(geometries, other.geometries);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + queryRelation.hashCode();
    h = 31 * h + Arrays.hashCode(geometries);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Weight weight = this;
        final SortedNumericDocValues values = context.reader().getSortedNumericDocValues(field);
        if (values == null) {
          return null;
        }
        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) {
            return new ConstantScoreScorer(weight, score(), scoreMode, getTwoPhaseIterator(values));
          }

          @Override
          public long cost() {
            return values.cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  private TwoPhaseIterator getTwoPhaseIterator(SortedNumericDocValues values) {
    // TODO: optimise for singleton doc values
    return switch (queryRelation) {
      case INTERSECTS -> intersects(values);
      case WITHIN -> within(values);
      case DISJOINT -> disjoint(values);
      case CONTAINS -> contains(values);
    };
  }

  private TwoPhaseIterator intersects(SortedNumericDocValues values) {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        GeoEncodingUtils.createComponentPredicate(LatLonGeometry.create(geometries));
    return new LatLonDocValuesTwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private TwoPhaseIterator within(SortedNumericDocValues values) {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        GeoEncodingUtils.createComponentPredicate(LatLonGeometry.create(geometries));
    return new LatLonDocValuesTwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon) == false) {
            return false;
          }
        }
        return true;
      }
    };
  }

  private TwoPhaseIterator disjoint(SortedNumericDocValues values) {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        GeoEncodingUtils.createComponentPredicate(LatLonGeometry.create(geometries));
    return new LatLonDocValuesTwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  private TwoPhaseIterator contains(SortedNumericDocValues values) {
    final List<Component2D> component2Ds = new ArrayList<>(geometries.length);
    for (LatLonGeometry geometry : geometries) {
      component2Ds.add(LatLonGeometry.create(geometry));
    }
    return new LatLonDocValuesTwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final double lat = GeoEncodingUtils.decodeLatitude((int) (value >>> 32));
          final double lon = GeoEncodingUtils.decodeLongitude((int) (value & 0xFFFFFFFF));
          for (Component2D component2D : component2Ds) {
            Component2D.WithinRelation relation = component2D.withinPoint(lon, lat);
            if (relation == Component2D.WithinRelation.NOTWITHIN) {
              return false;
            } else if (relation != Component2D.WithinRelation.DISJOINT) {
              answer = relation;
            }
          }
        }
        return answer == Component2D.WithinRelation.CANDIDATE;
      }
    };
  }

  private abstract static class LatLonDocValuesTwoPhaseIterator extends TwoPhaseIterator {
    protected LatLonDocValuesTwoPhaseIterator(DocIdSetIterator approximation) {
      super(approximation);
    }

    @Override
    public float matchCost() {
      return 1000f; // TODO: what should it be?
    }
  }
}
