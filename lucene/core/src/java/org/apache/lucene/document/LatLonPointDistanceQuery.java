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

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import java.io.IOException;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
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
import org.apache.lucene.util.NumericUtils;

/** Distance query for {@link LatLonPoint}. */
final class LatLonPointDistanceQuery extends Query {
  final String field;
  final double latitude;
  final double longitude;
  final double radiusMeters;

  public LatLonPointDistanceQuery(
      String field, double latitude, double longitude, double radiusMeters) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("radiusMeters: '" + radiusMeters + "' is invalid");
    }
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.radiusMeters = radiusMeters;
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
    Rectangle box = Rectangle.fromPointDistance(latitude, longitude, radiusMeters);
    // create bounding box(es) for the distance range
    // these are pre-encoded with LatLonPoint's encoding
    final int minLat = encodeLatitude(box.minLat);
    final int maxLat = encodeLatitude(box.maxLat);
    int minLon;
    int maxLon;
    // second set of longitude ranges to check (for cross-dateline case)
    int minLon2;

    // crosses dateline: split
    if (box.crossesDateline()) {
      // box1
      minLon = Integer.MIN_VALUE;
      maxLon = encodeLongitude(box.maxLon);
      // box2
      minLon2 = encodeLongitude(box.minLon);
    } else {
      minLon = encodeLongitude(box.minLon);
      maxLon = encodeLongitude(box.maxLon);
      // disable box2
      minLon2 = Integer.MAX_VALUE;
    }

    // compute exact sort key: avoid any asin() computations
    final double sortKey = GeoUtils.distanceQuerySortKey(radiusMeters);

    final double axisLat = Rectangle.axisLat(latitude, radiusMeters);

    return new ConstantScoreWeight(this, boost) {

      final GeoEncodingUtils.DistancePredicate distancePredicate =
          GeoEncodingUtils.createDistancePredicate(latitude, longitude, radiusMeters);

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        LatLonPoint.checkCompatible(fieldInfo);

        // matching docids
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
        final IntersectVisitor visitor = getIntersectVisitor(result);

        final Weight weight = this;
        return new ScorerSupplier() {

          long cost = -1;

          @Override
          public Scorer get(long leadCost) throws IOException {
            if (values.getDocCount() == reader.maxDoc()
                && values.getDocCount() == values.size()
                && cost() > reader.maxDoc() / 2) {
              // If all docs have exactly one value and the cost is greater
              // than half the leaf size then maybe we can make things faster
              // by computing the set of documents that do NOT match the range
              final FixedBitSet result = new FixedBitSet(reader.maxDoc());
              result.set(0, reader.maxDoc());
              long[] cost = new long[] {reader.maxDoc()};
              values.intersect(getInverseIntersectVisitor(result, cost));
              final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }
            values.intersect(visitor);
            return new ConstantScoreScorer(weight, score(), scoreMode, result.build().iterator());
          }

          @Override
          public long cost() {
            if (cost == -1) {
              cost = values.estimateDocCount(visitor);
            }
            assert cost >= 0;
            return cost;
          }
        };
      }

      private boolean matches(byte[] packedValue) {
        int lat = NumericUtils.sortableBytesToInt(packedValue, 0);
        // bounding box check
        if (lat > maxLat || lat < minLat) {
          // latitude out of bounding box range
          return false;
        }
        int lon = NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES);
        if ((lon > maxLon || lon < minLon) && lon < minLon2) {
          // longitude out of bounding box range
          return false;
        }
        return distancePredicate.test(lat, lon);
      }

      // algorithm: we create a bounding box (two bounding boxes if we cross the dateline).
      // 1. check our bounding box(es) first. if the subtree is entirely outside of those, bail.
      // 2. check if the subtree is disjoint. it may cross the bounding box but not intersect with
      // circle
      // 3. see if the subtree is fully contained. if the subtree is enormous along the x axis,
      // wrapping half way around the world, etc: then this can't work, just go to step 4.
      // 4. recurse naively (subtrees crossing over circle edge)
      private Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        int latLowerBound = NumericUtils.sortableBytesToInt(minPackedValue, 0);
        int latUpperBound = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
        if (latLowerBound > maxLat || latUpperBound < minLat) {
          // latitude out of bounding box range
          return Relation.CELL_OUTSIDE_QUERY;
        }

        int lonLowerBound = NumericUtils.sortableBytesToInt(minPackedValue, LatLonPoint.BYTES);
        int lonUpperBound = NumericUtils.sortableBytesToInt(maxPackedValue, LatLonPoint.BYTES);
        if ((lonLowerBound > maxLon || lonUpperBound < minLon) && lonUpperBound < minLon2) {
          // longitude out of bounding box range
          return Relation.CELL_OUTSIDE_QUERY;
        }

        double latMin = decodeLatitude(latLowerBound);
        double lonMin = decodeLongitude(lonLowerBound);
        double latMax = decodeLatitude(latUpperBound);
        double lonMax = decodeLongitude(lonUpperBound);

        return GeoUtils.relate(
            latMin, latMax, lonMin, lonMax, latitude, longitude, sortKey, axisLat);
      }

      /** Create a visitor that collects documents matching the range. */
      private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
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
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return relate(minPackedValue, maxPackedValue);
          }
        };
      }

      /** Create a visitor that clears documents that do NOT match the range. */
      private IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, long[] cost) {
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
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            Relation relation = relate(minPackedValue, maxPackedValue);
            switch (relation) {
              case CELL_INSIDE_QUERY:
                // all points match, skip this subtree
                return Relation.CELL_OUTSIDE_QUERY;
              case CELL_OUTSIDE_QUERY:
                // none of the points match, clear all documents
                return Relation.CELL_INSIDE_QUERY;
              case CELL_CROSSES_QUERY:
              default:
                return relation;
            }
          }
        };
      }
    };
  }

  public String getField() {
    return field;
  }

  public double getLatitude() {
    return latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getRadiusMeters() {
    return radiusMeters;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + field.hashCode();
    long temp;
    temp = Double.doubleToLongBits(latitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(longitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(radiusMeters);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LatLonPointDistanceQuery other) {
    return field.equals(other.field)
        && Double.doubleToLongBits(latitude) == Double.doubleToLongBits(other.latitude)
        && Double.doubleToLongBits(longitude) == Double.doubleToLongBits(other.longitude)
        && Double.doubleToLongBits(radiusMeters) == Double.doubleToLongBits(other.radiusMeters);
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (!this.field.equals(field)) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(latitude);
    sb.append(",");
    sb.append(longitude);
    sb.append(" +/- ");
    sb.append(radiusMeters);
    sb.append(" meters");
    return sb.toString();
  }
}
