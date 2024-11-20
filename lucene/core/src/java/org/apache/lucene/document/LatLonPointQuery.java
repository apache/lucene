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

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed geo points that comply the given {@link QueryRelation} with the
 * specified array of {@link LatLonGeometry}.
 *
 * <p>The field must be indexed using one or more {@link LatLonPoint} added per document.
 */
final class LatLonPointQuery extends SpatialQuery {

  /**
   * Creates a query that matches all indexed shapes to the provided array of {@link LatLonGeometry}
   */
  LatLonPointQuery(String field, QueryRelation queryRelation, LatLonGeometry... geometries) {
    super(field, queryRelation, validateGeometry(queryRelation, geometries));
  }

  private static LatLonGeometry[] validateGeometry(
      QueryRelation queryRelation, LatLonGeometry... geometries) {
    if (geometries != null) {
      if (queryRelation == QueryRelation.WITHIN) {
        for (LatLonGeometry geometry : geometries) {
          if (geometry instanceof Line) {
            // TODO: line queries do not support within relations
            throw new IllegalArgumentException(
                "LatLonPointQuery does not support "
                    + QueryRelation.WITHIN
                    + " queries with line geometries");
          }
        }
      }
      if (queryRelation == ShapeField.QueryRelation.CONTAINS) {
        for (LatLonGeometry geometry : geometries) {
          if ((geometry instanceof Point) == false) {
            throw new IllegalArgumentException(
                "LatLonPointQuery does not support "
                    + ShapeField.QueryRelation.CONTAINS
                    + " queries with non-points geometries");
          }
        }
      }
    }

    return geometries;
  }

  @Override
  protected Component2D createComponent2D(Geometry... geometries) {
    return LatLonGeometry.create((LatLonGeometry[]) geometries);
  }

  @Override
  protected SpatialVisitor getSpatialVisitor() {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        GeoEncodingUtils.createComponentPredicate(queryComponent2D);
    // bounding box over all geometries, this can speed up tree intersection/cheaply improve
    // approximation for complex multi-geometries
    final int minLat = encodeLatitude(queryComponent2D.getMinY());
    final int maxLat = encodeLatitude(queryComponent2D.getMaxY());
    final int minLon = encodeLongitude(queryComponent2D.getMinX());
    final int maxLon = encodeLongitude(queryComponent2D.getMaxX());

    return new SpatialVisitor() {

      @Override
      protected Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        int latLowerBound = NumericUtils.sortableBytesToInt(minPackedValue, 0);
        int latUpperBound = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
        if (latLowerBound > maxLat || latUpperBound < minLat) {
          // outside of global bounding box range
          return Relation.CELL_OUTSIDE_QUERY;
        }

        int lonLowerBound = NumericUtils.sortableBytesToInt(minPackedValue, LatLonPoint.BYTES);
        int lonUpperBound = NumericUtils.sortableBytesToInt(maxPackedValue, LatLonPoint.BYTES);
        if (lonLowerBound > maxLon || lonUpperBound < minLon) {
          // outside of global bounding box range
          return Relation.CELL_OUTSIDE_QUERY;
        }

        double cellMinLat = decodeLatitude(latLowerBound);
        double cellMinLon = decodeLongitude(lonLowerBound);
        double cellMaxLat = decodeLatitude(latUpperBound);
        double cellMaxLon = decodeLongitude(lonUpperBound);

        return queryComponent2D.relate(cellMinLon, cellMaxLon, cellMinLat, cellMaxLat);
      }

      @Override
      protected Predicate<byte[]> intersects() {
        return packedValue ->
            component2DPredicate.test(
                NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES));
      }

      @Override
      protected Predicate<byte[]> within() {
        return packedValue ->
            component2DPredicate.test(
                NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES));
      }

      @Override
      protected Function<byte[], Component2D.WithinRelation> contains() {
        return packedValue ->
            queryComponent2D.withinPoint(
                GeoEncodingUtils.decodeLongitude(
                    NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES)),
                GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(packedValue, 0)));
      }
    };
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

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(geometries, ((LatLonPointQuery) o).geometries);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(geometries);
    return hash;
  }
}
