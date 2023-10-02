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

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.ShapeField.QueryRelation; // javadoc
import org.apache.lucene.document.ShapeField.Triangle;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues; // javadoc
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * An geo shape utility class for indexing and searching gis geometries whose vertices are latitude,
 * longitude values (in decimal degrees).
 *
 * <p>This class defines seven static factory methods for common indexing and search operations:
 *
 * <ul>
 *   <li>{@link #createIndexableFields(String, Polygon)} for indexing a geo polygon.
 *   <li>{@link #createDocValueField(String, Polygon)} for indexing a geo polygon doc value field.
 *   <li>{@link #createIndexableFields(String, Polygon, boolean)} for indexing a geo polygon with
 *       the possibility of checking for self-intersections.
 *   <li>{@link #createIndexableFields(String, Polygon, boolean)} for indexing a geo polygon doc
 *       value field with the possibility of checking for self-intersections.
 *   <li>{@link #createIndexableFields(String, Line)} for indexing a geo linestring.
 *   <li>{@link #createDocValueField(String, Line)} for indexing a geo linestring doc value.
 *   <li>{@link #createIndexableFields(String, double, double)} for indexing a lat, lon geo point.
 *   <li>{@link #createDocValueField(String, double, double)} for indexing a lat, lon geo point doc
 *       value.
 *   <li>{@link #createDocValueField(String, BytesRef)} for indexing a cartesian doc value from
 *       existing encoding.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching geo shapes that have some {@link
 *       QueryRelation} with a bounding box.
 *   <li>{@link #newLineQuery newLineQuery()} for matching geo shapes that have some {@link
 *       QueryRelation} with a linestring.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching geo shapes that have some {@link
 *       QueryRelation} with a polygon.
 *   <li>{@link #newGeometryQuery newGeometryQuery()} for matching geo shapes that have some {@link
 *       QueryRelation} with one or more {@link LatLonGeometry}.
 *   <li>{@link #createLatLonShapeDocValues(BytesRef)} for creating the {@link LatLonShapeDocValues}
 * </ul>
 *
 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision
 * from the original {@code double} values (4.190951585769653E-8 for the latitude component and
 * 8.381903171539307E-8 for longitude).
 *
 * @see PointValues
 * @see LatLonDocValuesField
 */
public class LatLonShape {

  // no instance:
  private LatLonShape() {}

  /** create indexable fields for polygon geometry. */
  public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
    return createIndexableFields(fieldName, polygon, false);
  }

  /** create doc value field for lat lon polygon geometry without creating indexable fields */
  public static LatLonShapeDocValuesField createDocValueField(String fieldName, Polygon polygon) {
    return createDocValueField(fieldName, polygon, false);
  }

  /**
   * create indexable fields for polygon geometry. If {@code checkSelfIntersections} is set to true,
   * the validity of the provided polygon is checked with a small performance penalty.
   */
  public static Field[] createIndexableFields(
      String fieldName, Polygon polygon, boolean checkSelfIntersections) {
    // the lionshare of the indexing is done by the tessellator
    List<Tessellator.Triangle> tessellation =
        Tessellator.tessellate(polygon, checkSelfIntersections);
    Triangle[] fields = new Triangle[tessellation.size()];
    for (int i = 0; i < tessellation.size(); i++) {
      fields[i] = new Triangle(fieldName, tessellation.get(i));
    }
    return fields;
  }

  /** create doc value field for lat lon polygon geometry without creating indexable fields. */
  public static LatLonShapeDocValuesField createDocValueField(
      String fieldName, Polygon polygon, boolean checkSelfIntersections) {
    List<Tessellator.Triangle> tessellation =
        Tessellator.tessellate(polygon, checkSelfIntersections);
    ArrayList<ShapeField.DecodedTriangle> triangles = new ArrayList<>(tessellation.size());
    for (Tessellator.Triangle t : tessellation) {
      ShapeField.DecodedTriangle dt = new ShapeField.DecodedTriangle();
      dt.type = ShapeField.DecodedTriangle.TYPE.TRIANGLE;
      dt.setValues(
          t.getEncodedX(0),
          t.getEncodedY(0),
          t.isEdgefromPolygon(0),
          t.getEncodedX(1),
          t.getEncodedY(1),
          t.isEdgefromPolygon(0),
          t.getEncodedX(2),
          t.getEncodedY(2),
          t.isEdgefromPolygon(2));
      triangles.add(dt);
    }
    return new LatLonShapeDocValuesField(fieldName, triangles);
  }

  /** create indexable fields for line geometry */
  public static Field[] createIndexableFields(String fieldName, Line line) {
    int numPoints = line.numPoints();
    Field[] fields = new Field[numPoints - 1];
    // create "flat" triangles
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      fields[i] =
          new Triangle(
              fieldName,
              encodeLongitude(line.getLon(i)),
              encodeLatitude(line.getLat(i)),
              encodeLongitude(line.getLon(j)),
              encodeLatitude(line.getLat(j)),
              encodeLongitude(line.getLon(i)),
              encodeLatitude(line.getLat(i)));
    }
    return fields;
  }

  /** create doc value field for lat lon line geometry without creating indexable fields. */
  public static LatLonShapeDocValuesField createDocValueField(String fieldName, Line line) {
    int numPoints = line.numPoints();
    List<ShapeField.DecodedTriangle> triangles = new ArrayList<>(numPoints - 1);
    // create "flat" triangles
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      ShapeField.DecodedTriangle t = new ShapeField.DecodedTriangle();
      t.type = ShapeField.DecodedTriangle.TYPE.LINE;
      t.setValues(
          encodeLongitude(line.getLon(i)),
          encodeLatitude(line.getLat(i)),
          true,
          encodeLongitude(line.getLon(j)),
          encodeLatitude(line.getLat(j)),
          true,
          encodeLongitude(line.getLon(i)),
          encodeLatitude(line.getLat(i)),
          true);
      triangles.add(t);
    }
    return new LatLonShapeDocValuesField(fieldName, triangles);
  }

  /** create indexable fields for point geometry */
  public static Field[] createIndexableFields(String fieldName, double lat, double lon) {
    return new Field[] {
      new Triangle(
          fieldName,
          encodeLongitude(lon),
          encodeLatitude(lat),
          encodeLongitude(lon),
          encodeLatitude(lat),
          encodeLongitude(lon),
          encodeLatitude(lat))
    };
  }

  /** create doc value field for lat lon line geometry without creating indexable fields. */
  public static LatLonShapeDocValuesField createDocValueField(
      String fieldName, double lat, double lon) {
    List<ShapeField.DecodedTriangle> triangles = new ArrayList<>(1);
    ShapeField.DecodedTriangle t = new ShapeField.DecodedTriangle();
    t.type = ShapeField.DecodedTriangle.TYPE.POINT;
    t.setValues(
        encodeLongitude(lon),
        encodeLatitude(lat),
        true,
        encodeLongitude(lon),
        encodeLatitude(lat),
        true,
        encodeLongitude(lon),
        encodeLatitude(lat),
        true);
    triangles.add(t);
    return new LatLonShapeDocValuesField(fieldName, triangles);
  }

  /** create a {@link LatLonShapeDocValuesField} from an existing encoded representation */
  public static LatLonShapeDocValuesField createDocValueField(
      String fieldName, BytesRef binaryValue) {
    return new LatLonShapeDocValuesField(fieldName, binaryValue);
  }

  /** create a {@link LatLonShapeDocValuesField} from an existing tessellation */
  public static LatLonShapeDocValuesField createDocValueField(
      String fieldName, List<ShapeField.DecodedTriangle> tessellation) {
    return new LatLonShapeDocValuesField(fieldName, tessellation);
  }

  /** create a shape docvalue field from indexable fields */
  public static LatLonShapeDocValuesField createDocValueField(
      String fieldName, Field[] indexableFields) {
    ArrayList<ShapeField.DecodedTriangle> tess = new ArrayList<>(indexableFields.length);
    final byte[] scratch = new byte[7 * Integer.BYTES];
    for (Field f : indexableFields) {
      BytesRef br = f.binaryValue();
      assert br.length == 7 * ShapeField.BYTES;
      System.arraycopy(br.bytes, br.offset, scratch, 0, 7 * ShapeField.BYTES);
      ShapeField.DecodedTriangle t = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(scratch, t);
      tess.add(t);
    }
    return new LatLonShapeDocValuesField(fieldName, tess);
  }

  /** create a query to find all indexed geo shapes that intersect a defined bounding box * */
  public static Query newBoxQuery(
      String field,
      QueryRelation queryRelation,
      double minLatitude,
      double maxLatitude,
      double minLongitude,
      double maxLongitude) {
    if (queryRelation == QueryRelation.CONTAINS && minLongitude > maxLongitude) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(
          newBoxQuery(
              field, queryRelation, minLatitude, maxLatitude, minLongitude, GeoUtils.MAX_LON_INCL),
          BooleanClause.Occur.MUST);
      builder.add(
          newBoxQuery(
              field, queryRelation, minLatitude, maxLatitude, GeoUtils.MIN_LON_INCL, maxLongitude),
          BooleanClause.Occur.MUST);
      return builder.build();
    }
    Rectangle rectangle = new Rectangle(minLatitude, maxLatitude, minLongitude, maxLongitude);
    return new LatLonShapeBoundingBoxQuery(field, queryRelation, rectangle);
  }

  /** create a docvalue query to find all geo shapes that intersect a defined bounding box * */
  public static Query newSlowDocValuesBoxQuery(
      String field,
      QueryRelation queryRelation,
      double minLatitude,
      double maxLatitude,
      double minLongitude,
      double maxLongitude) {
    if (queryRelation == QueryRelation.CONTAINS && minLongitude > maxLongitude) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(
          newBoxQuery(
              field, queryRelation, minLatitude, maxLatitude, minLongitude, GeoUtils.MAX_LON_INCL),
          BooleanClause.Occur.MUST);
      builder.add(
          newBoxQuery(
              field, queryRelation, minLatitude, maxLatitude, GeoUtils.MIN_LON_INCL, maxLongitude),
          BooleanClause.Occur.MUST);
      return builder.build();
    }
    return new LatLonShapeDocValuesQuery(
        field, queryRelation, new Rectangle(minLatitude, maxLatitude, minLongitude, maxLongitude));
  }

  /**
   * create a query to find all indexed geo shapes that intersect a provided linestring (or array of
   * linestrings) note: does not support dateline crossing
   */
  public static Query newLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    return newGeometryQuery(field, queryRelation, lines);
  }

  /**
   * create a query to find all indexed geo shapes that intersect a provided polygon (or array of
   * polygons) note: does not support dateline crossing
   */
  public static Query newPolygonQuery(
      String field, QueryRelation queryRelation, Polygon... polygons) {
    return newGeometryQuery(field, queryRelation, polygons);
  }

  /**
   * create a query to find all indexed shapes that comply the {@link QueryRelation} with the
   * provided points
   */
  public static Query newPointQuery(String field, QueryRelation queryRelation, double[]... points) {
    Point[] pointArray = new Point[points.length];
    for (int i = 0; i < points.length; i++) {
      pointArray[i] = new Point(points[i][0], points[i][1]);
    }
    return newGeometryQuery(field, queryRelation, pointArray);
  }

  /** create a query to find all polygons that intersect a provided circle. */
  public static Query newDistanceQuery(
      String field, QueryRelation queryRelation, Circle... circle) {
    return newGeometryQuery(field, queryRelation, circle);
  }

  /**
   * create a query to find all indexed geo shapes that intersect a provided geometry (or array of
   * geometries).
   */
  public static Query newGeometryQuery(
      String field, QueryRelation queryRelation, LatLonGeometry... latLonGeometries) {
    if (latLonGeometries.length == 1) {
      LatLonGeometry geometry = latLonGeometries[0];
      if (geometry instanceof Rectangle) {
        Rectangle rect = (Rectangle) geometry;
        return newBoxQuery(
            field, queryRelation, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
      } else {
        return new LatLonShapeQuery(field, queryRelation, latLonGeometries);
      }
    } else {
      if (queryRelation == QueryRelation.CONTAINS) {
        return makeContainsGeometryQuery(field, latLonGeometries);
      } else {
        return new LatLonShapeQuery(field, queryRelation, latLonGeometries);
      }
    }
  }

  /**
   * Factory method for creating the {@link LatLonShapeDocValues}
   *
   * @param bytesRef {@link BytesRef}
   * @return {@link LatLonShapeDocValues}
   */
  public static LatLonShapeDocValues createLatLonShapeDocValues(BytesRef bytesRef) {
    return new LatLonShapeDocValues(bytesRef);
  }

  private static Query makeContainsGeometryQuery(String field, LatLonGeometry... latLonGeometries) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (LatLonGeometry geometry : latLonGeometries) {
      if (geometry instanceof Rectangle) {
        // this handles rectangles across the dateline
        Rectangle rect = (Rectangle) geometry;
        builder.add(
            newBoxQuery(
                field, QueryRelation.CONTAINS, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon),
            BooleanClause.Occur.MUST);
      } else {
        builder.add(
            new LatLonShapeQuery(field, QueryRelation.CONTAINS, geometry),
            BooleanClause.Occur.MUST);
      }
    }
    return new ConstantScoreQuery(builder.build());
  }
}
