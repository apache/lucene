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
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_INCL;

import java.io.IOException;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;

/**
 * Bounding Box query for {@link ShapeDocValuesField} representing {@link XYShape}
 *
 * @lucene.experimental
 */
final class LatLonShapeDocValuesBoundingBoxQuery extends BaseShapeDocValuesBoundingBoxQuery {
  protected final boolean crossesDateline;

  LatLonShapeDocValuesBoundingBoxQuery(
      String field,
      QueryRelation queryRelation,
      double minLatitude,
      double maxLatitude,
      double minLongitude,
      double maxLongitude) {
    this(
        field,
        queryRelation,
        validateMinLon(minLongitude, maxLongitude) > maxLongitude,
        minLatitude,
        maxLatitude,
        validateMinLon(minLongitude, maxLongitude),
        maxLongitude);
  }

  LatLonShapeDocValuesBoundingBoxQuery(
      String field,
      ShapeField.QueryRelation queryRelation,
      boolean crossesDateline,
      double minLatitude,
      double maxLatitude,
      double minLongitude,
      double maxLongitude) {
    super(
        field,
        queryRelation,
        transformLongitude(minLongitude, true),
        transformLongitude(maxLongitude, false),
        transformLatitude(minLatitude, true),
        transformLatitude(maxLatitude, false));
    this.crossesDateline = crossesDateline;
  }

  private static int transformLongitude(double longitude, boolean ceil) {
    GeoUtils.checkLongitude(longitude);
    if (ceil == true) {
      // transform using the ceiling function
      return GeoEncodingUtils.encodeLongitudeCeil(longitude);
    }
    return GeoEncodingUtils.encodeLongitude(longitude);
  }

  private static int transformLatitude(double latitude, boolean ceil) {
    GeoUtils.checkLatitude(latitude);
    if (ceil == true) {
      return GeoEncodingUtils.encodeLatitudeCeil(latitude);
    }
    return GeoEncodingUtils.encodeLatitude(latitude);
  }

  /** returns a valid minLon (-180) if the bbox splits the dateline */
  private static double validateMinLon(double minLon, double maxLon) {
    if (minLon == 180.0 && minLon > maxLon) {
      return -180D;
    }
    return minLon;
  }

  @Override
  public boolean match(ShapeDocValuesField shapeDocValues) throws IOException {
    boolean result;
    if (crossesDateline == false) {
      return super.match(shapeDocValues);
    } else {
      result =
          matchesBox(shapeDocValues, queryRelation, minX, MAX_LON_ENCODED, minY, maxY)
              || matchesBox(
                  shapeDocValues,
                  queryRelation,
                  encodeLongitudeCeil(MIN_LON_INCL),
                  maxX,
                  minY,
                  maxY);
    }
    if (queryRelation == ShapeField.QueryRelation.DISJOINT) {
      return result == false;
    }

    return result;
  }

  @Override
  public float matchCost() {
    // multiply number of terms by worst number of comparisons (estimated 60 comparisons)
    // multiply by two if we're comparing east and west bounding boxes
    // todo: revisit?
    return 60 * (crossesDateline ? 2 : 1) * 100;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = super.equals(obj);
    LatLonShapeDocValuesBoundingBoxQuery other = (LatLonShapeDocValuesBoundingBoxQuery) obj;
    return equals && crossesDateline == other.crossesDateline;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Boolean.hashCode(crossesDateline);
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("box(minLat=").append(GeoEncodingUtils.decodeLatitude(minY));
    sb.append(", maxLat=").append(GeoEncodingUtils.decodeLatitude(maxY));
    sb.append(", minLon=").append(GeoEncodingUtils.decodeLongitude(minX));
    sb.append(", maxLon=").append(GeoEncodingUtils.decodeLongitude(maxX));
    return sb.append(")").toString();
  }
}
