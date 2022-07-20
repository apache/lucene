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

import static java.lang.Integer.BYTES;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed geo shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using {@link
 * org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 *
 * @lucene.internal
 */
final class LatLonShapeBoundingBoxQuery extends SpatialQuery {
  private final Rectangle rectangle;

  LatLonShapeBoundingBoxQuery(String field, QueryRelation queryRelation, Rectangle rectangle) {
    super(field, queryRelation, rectangle);
    this.rectangle = rectangle;
  }

  @Override
  protected Component2D createComponent2D(Geometry... geometries) {
    // todo: this isn't actually used by the query so maybe we can just return null?
    return LatLonGeometry.create((Rectangle) geometries[0]);
  }

  @Override
  protected SpatialVisitor getSpatialVisitor() {
    final EncodedLatLonRectangle encodedRectangle =
        new EncodedLatLonRectangle(
            rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    return new SpatialVisitor() {

      @Override
      protected Relation relate(byte[] minTriangle, byte[] maxTriangle) {
        if (queryRelation == QueryRelation.INTERSECTS || queryRelation == QueryRelation.DISJOINT) {
          return encodedRectangle.intersectRangeBBox(
              ShapeField.BYTES,
              0,
              minTriangle,
              3 * ShapeField.BYTES,
              2 * ShapeField.BYTES,
              maxTriangle);
        }
        return encodedRectangle.relateRangeBBox(
            ShapeField.BYTES,
            0,
            minTriangle,
            3 * ShapeField.BYTES,
            2 * ShapeField.BYTES,
            maxTriangle);
      }

      @Override
      protected Predicate<byte[]> intersects() {
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
              {
                return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
              }
            case LINE:
              {
                int aY = scratchTriangle.aY;
                int aX = scratchTriangle.aX;
                int bY = scratchTriangle.bY;
                int bX = scratchTriangle.bX;
                return encodedRectangle.intersectsLine(aX, aY, bX, bY);
              }
            case TRIANGLE:
              {
                int aY = scratchTriangle.aY;
                int aX = scratchTriangle.aX;
                int bY = scratchTriangle.bY;
                int bX = scratchTriangle.bX;
                int cY = scratchTriangle.cY;
                int cX = scratchTriangle.cX;
                return encodedRectangle.intersectsTriangle(aX, aY, bX, bY, cX, cY);
              }
            default:
              throw new IllegalArgumentException(
                  "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }

      @Override
      protected Predicate<byte[]> within() {
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
              {
                return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
              }
            case LINE:
              {
                int aY = scratchTriangle.aY;
                int aX = scratchTriangle.aX;
                int bY = scratchTriangle.bY;
                int bX = scratchTriangle.bX;
                return encodedRectangle.containsLine(aX, aY, bX, bY);
              }
            case TRIANGLE:
              {
                int aY = scratchTriangle.aY;
                int aX = scratchTriangle.aX;
                int bY = scratchTriangle.bY;
                int bX = scratchTriangle.bX;
                int cY = scratchTriangle.cY;
                int cX = scratchTriangle.cX;
                return encodedRectangle.containsTriangle(aX, aY, bX, bY, cX, cY);
              }
            default:
              throw new IllegalArgumentException(
                  "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }

      @Override
      protected Function<byte[], Component2D.WithinRelation> contains() {
        if (encodedRectangle.crossesDateline()) {
          throw new IllegalArgumentException(
              "withinTriangle is not supported for rectangles crossing the date line");
        }
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {

          // decode indexed triangle
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
              {
                return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY)
                    ? Component2D.WithinRelation.NOTWITHIN
                    : Component2D.WithinRelation.DISJOINT;
              }
            case LINE:
              {
                return encodedRectangle.withinLine(
                    scratchTriangle.aX,
                    scratchTriangle.aY,
                    scratchTriangle.ab,
                    scratchTriangle.bX,
                    scratchTriangle.bY);
              }
            case TRIANGLE:
              {
                return encodedRectangle.withinTriangle(
                    scratchTriangle.aX,
                    scratchTriangle.aY,
                    scratchTriangle.ab,
                    scratchTriangle.bX,
                    scratchTriangle.bY,
                    scratchTriangle.bc,
                    scratchTriangle.cX,
                    scratchTriangle.cY,
                    scratchTriangle.ca);
              }
            default:
              throw new IllegalArgumentException(
                  "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }
    };
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle.equals(((LatLonShapeBoundingBoxQuery) o).rectangle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle.hashCode();
    return hash;
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
    sb.append(rectangle.toString());
    return sb.toString();
  }

  /** Holds spatial logic for a bounding box that works in the encoded space */
  private static class EncodedLatLonRectangle extends EncodedRectangle {
    protected final byte[] bbox;
    private final byte[] west;

    EncodedLatLonRectangle(double minLat, double maxLat, double minLon, double maxLon) {
      super(
          encodeLongitudeCeil(validateMinLon(minLon, maxLon)),
          encodeLongitude(maxLon),
          encodeLatitudeCeil(minLat),
          encodeLatitude(maxLat),
          validateMinLon(minLon, maxLon) > maxLon);
      this.bbox = new byte[4 * BYTES];

      if (wrapsCoordinateSystem) {
        // crossing dateline is split into east/west boxes
        this.west = new byte[4 * BYTES];
        encode(MIN_LON_ENCODED, this.maxX, this.minY, this.maxY, this.west);
        encode(this.minX, MAX_LON_ENCODED, this.minY, this.maxY, this.bbox);
      } else {
        this.west = null;
        encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
      }
    }

    /** returns a valid minLon (-180) if the bbox splits the dateline */
    private static double validateMinLon(double minLon, double maxLon) {
      if (minLon == 180.0 && minLon > maxLon) {
        return -180D;
      }
      return minLon;
    }

    /** encodes a bounding box into the provided byte array */
    private static void encode(
        final int minX, final int maxX, final int minY, final int maxY, byte[] b) {
      if (b == null) {
        b = new byte[4 * BYTES];
      }
      NumericUtils.intToSortableBytes(minY, b, 0);
      NumericUtils.intToSortableBytes(minX, b, BYTES);
      NumericUtils.intToSortableBytes(maxY, b, 2 * BYTES);
      NumericUtils.intToSortableBytes(maxX, b, 3 * BYTES);
    }

    private boolean crossesDateline() {
      return wrapsCoordinateSystem;
    }

    /** compare this to a provided range bounding box */
    Relation relateRangeBBox(
        int minXOffset,
        int minYOffset,
        byte[] minTriangle,
        int maxXOffset,
        int maxYOffset,
        byte[] maxTriangle) {
      Relation eastRelation =
          compareBBoxToRangeBBox(
              this.bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return compareBBoxToRangeBBox(
            this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /** intersects this to a provided range bounding box */
    Relation intersectRangeBBox(
        int minXOffset,
        int minYOffset,
        byte[] minTriangle,
        int maxXOffset,
        int maxYOffset,
        byte[] maxTriangle) {
      Relation eastRelation =
          intersectBBoxWithRangeBBox(
              this.bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return intersectBBoxWithRangeBBox(
            this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the
     * triangle collection)
     */
    private Relation compareBBoxToRangeBBox(
        final byte[] bbox,
        int minXOffset,
        int minYOffset,
        byte[] minTriangle,
        int maxXOffset,
        int maxYOffset,
        byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(
          bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (ArrayUtil.compareUnsigned4(minTriangle, minXOffset, bbox, BYTES) >= 0
          && ArrayUtil.compareUnsigned4(maxTriangle, maxXOffset, bbox, 3 * BYTES) <= 0
          && ArrayUtil.compareUnsigned4(minTriangle, minYOffset, bbox, 0) >= 0
          && ArrayUtil.compareUnsigned4(maxTriangle, maxYOffset, bbox, 2 * BYTES) <= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the
     * triangle collection) for intersection
     */
    private Relation intersectBBoxWithRangeBBox(
        final byte[] bbox,
        int minXOffset,
        int minYOffset,
        byte[] minTriangle,
        int maxXOffset,
        int maxYOffset,
        byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(
          bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (ArrayUtil.compareUnsigned4(minTriangle, minXOffset, bbox, BYTES) >= 0
          && ArrayUtil.compareUnsigned4(minTriangle, minYOffset, bbox, 0) >= 0) {
        if (ArrayUtil.compareUnsigned4(maxTriangle, minXOffset, bbox, 3 * BYTES) <= 0
            && ArrayUtil.compareUnsigned4(maxTriangle, maxYOffset, bbox, 2 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (ArrayUtil.compareUnsigned4(maxTriangle, maxXOffset, bbox, 3 * BYTES) <= 0
            && ArrayUtil.compareUnsigned4(maxTriangle, minYOffset, bbox, 2 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      if (ArrayUtil.compareUnsigned4(maxTriangle, maxXOffset, bbox, 3 * BYTES) <= 0
          && ArrayUtil.compareUnsigned4(maxTriangle, maxYOffset, bbox, 2 * BYTES) <= 0) {
        if (ArrayUtil.compareUnsigned4(minTriangle, minXOffset, bbox, BYTES) >= 0
            && ArrayUtil.compareUnsigned4(minTriangle, maxYOffset, bbox, 0) >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (ArrayUtil.compareUnsigned4(minTriangle, maxXOffset, bbox, BYTES) >= 0
            && ArrayUtil.compareUnsigned4(minTriangle, minYOffset, bbox, 0) >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /** static utility method to check a bbox is disjoint with a range of triangles */
    private boolean disjoint(
        final byte[] bbox,
        int minXOffset,
        int minYOffset,
        byte[] minTriangle,
        int maxXOffset,
        int maxYOffset,
        byte[] maxTriangle) {
      return ArrayUtil.compareUnsigned4(minTriangle, minXOffset, bbox, 3 * BYTES) > 0
          || ArrayUtil.compareUnsigned4(maxTriangle, maxXOffset, bbox, BYTES) < 0
          || ArrayUtil.compareUnsigned4(minTriangle, minYOffset, bbox, 2 * BYTES) > 0
          || ArrayUtil.compareUnsigned4(maxTriangle, maxYOffset, bbox, 0) < 0;
    }
  }
}
