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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.apache.lucene.document.ShapeField.DecodedTriangle.TYPE;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.SimpleWKTShapeParser;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Simple tests for {@link org.apache.lucene.document.ShapeDocValuesField} */
public class TestShapeDocValues extends LuceneTestCase {
  private static final double TOLERANCE = 1E-7;

  private static final String FIELD_NAME = "field";

  public void testSimpleDocValue() throws Exception {
    ShapeDocValues dv = new LatLonShapeDocValues(getTessellation(getTestPolygonWithHole()));
    // tests geometry inside a hole and crossing
    assertEquals(
        dv.relate(LatLonGeometry.create(new Rectangle(-0.25, -0.24, -3.8, -3.7))),
        PointValues.Relation.CELL_OUTSIDE_QUERY);
    assertNotEquals(
        dv.relate(LatLonGeometry.create(new Rectangle(-1.2, 1.2, -1.5, 1.7))),
        PointValues.Relation.CELL_CROSSES_QUERY);
  }

  public void testLatLonPolygonBBox() {
    Polygon p = GeoTestUtil.nextPolygon();
    if (area(p) != 0) {
      Rectangle expected = (Rectangle) computeBoundingBox(p);
      LatLonShapeDocValuesField dv = LatLonShape.createDocValueField(FIELD_NAME, p);
      assertEquals(expected.minLat, dv.getBoundingBox().minLat, TOLERANCE);
      assertEquals(expected.maxLat, dv.getBoundingBox().maxLat, TOLERANCE);
      assertEquals(expected.minLon, dv.getBoundingBox().minLon, TOLERANCE);
      assertEquals(expected.maxLon, dv.getBoundingBox().maxLon, TOLERANCE);
    }
  }

  public void testXYPolygonBBox() {
    XYPolygon p = (XYPolygon) BaseXYShapeTestCase.ShapeType.POLYGON.nextShape();
    XYRectangle expected = (XYRectangle) computeBoundingBox(p);
    XYShapeDocValuesField dv = XYShape.createDocValueField(FIELD_NAME, p);
    assertEquals(expected.minX, dv.getBoundingBox().minX, TOLERANCE);
    assertEquals(expected.maxX, dv.getBoundingBox().maxX, TOLERANCE);
    assertEquals(expected.minY, dv.getBoundingBox().minY, TOLERANCE);
    assertEquals(expected.maxY, dv.getBoundingBox().maxY, TOLERANCE);
  }

  public void testLatLonPolygonCentroid() {
    Polygon p = GeoTestUtil.nextPolygon();
    Point expected = (Point) computeCentroid(p);
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    LatLonShapeDocValuesField dvField = LatLonShape.createDocValueField(FIELD_NAME, p);
    assertEquals(tess.size(), dvField.numberOfTerms());
    assertEquals(expected.getLat(), dvField.getCentroid().getLat(), TOLERANCE);
    assertEquals(expected.getLon(), dvField.getCentroid().getLon(), TOLERANCE);
    assertEquals(TYPE.TRIANGLE, dvField.getHighestDimensionType());
  }

  public void testXYPolygonCentroid() {
    XYPolygon p = (XYPolygon) BaseXYShapeTestCase.ShapeType.POLYGON.nextShape();
    XYPoint expected = (XYPoint) computeCentroid(p);
    XYShapeDocValuesField dvField = XYShape.createDocValueField(FIELD_NAME, getTessellation(p));
    assertEquals(expected.getX(), dvField.getCentroid().getX(), TOLERANCE);
    assertEquals(expected.getY(), dvField.getCentroid().getY(), TOLERANCE);
    assertEquals(TYPE.TRIANGLE, dvField.getHighestDimensionType());
  }

  private Geometry computeCentroid(Geometry p) {
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    double totalSignedArea = 0;
    double numXPly = 0;
    double numYPly = 0;
    double ax, bx, cx;
    double ay, by, cy;
    IntFunction<Double> decodeX =
        p instanceof Polygon
            ? (x) -> GeoEncodingUtils.decodeLongitude(x)
            : (x) -> (double) XYEncodingUtils.decode(x);
    IntFunction<Double> decodeY =
        p instanceof Polygon
            ? (y) -> GeoEncodingUtils.decodeLatitude(y)
            : (y) -> (double) XYEncodingUtils.decode(y);
    BiFunction<Double, Double, Geometry> createPoint =
        p instanceof Polygon
            ? (x, y) -> new Point(y, x)
            : (x, y) -> new XYPoint(x.floatValue(), y.floatValue());

    for (ShapeField.DecodedTriangle t : tess) {
      ax = decodeX.apply(t.aX);
      ay = decodeY.apply(t.aY);
      bx = decodeX.apply(t.bX);
      by = decodeY.apply(t.bY);
      cx = decodeX.apply(t.cX);
      cy = decodeY.apply(t.cY);

      double signedArea = Math.abs(0.5d * ((bx - ax) * (cy - ay) - (cx - ax) * (by - ay)));
      // accumulate midPoints and signed area
      numXPly += (((ax + bx + cx) / 3d) * signedArea);
      numYPly += (((ay + by + cy) / 3d) * signedArea);
      totalSignedArea += signedArea;
    }
    totalSignedArea = totalSignedArea == 0d ? 1 : totalSignedArea;
    return createPoint.apply(numXPly / totalSignedArea, numYPly / totalSignedArea);
  }

  /**
   * compute the bounding box from the tessellation; test utils may create self crossing polygons
   * cleaned by the tessellator
   */
  private Geometry computeBoundingBox(Geometry p) {
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    IntFunction<Double> decodeX =
        p instanceof Polygon
            ? (x) -> GeoEncodingUtils.decodeLongitude(x)
            : (x) -> (double) XYEncodingUtils.decode(x);
    IntFunction<Double> decodeY =
        p instanceof Polygon
            ? (y) -> GeoEncodingUtils.decodeLatitude(y)
            : (y) -> (double) XYEncodingUtils.decode(y);
    BiFunction<Double[], Double[], Geometry> createRectangle =
        p instanceof Polygon
            ? (min, max) -> new Rectangle(min[1], max[1], min[0], max[0])
            : (min, max) ->
                new XYRectangle(
                    min[0].floatValue(),
                    max[0].floatValue(),
                    min[1].floatValue(),
                    max[1].floatValue());

    double ax, bx, cx;
    double ay, by, cy;
    double minX = Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;
    for (ShapeField.DecodedTriangle t : tess) {
      ax = decodeX.apply(t.aX);
      ay = decodeY.apply(t.aY);
      bx = decodeX.apply(t.bX);
      by = decodeY.apply(t.bY);
      cx = decodeX.apply(t.cX);
      cy = decodeY.apply(t.cY);
      minX = Math.min(minX, Math.min(ax, Math.min(bx, cx)));
      maxX = Math.max(maxX, Math.max(ax, Math.max(bx, cx)));
      minY = Math.min(minY, Math.min(ay, Math.min(by, cy)));
      maxY = Math.max(maxY, Math.max(ay, Math.max(by, cy)));
    }
    return createRectangle.apply(new Double[] {minX, minY}, new Double[] {maxX, maxY});
  }

  public void testExplicitLatLonPolygonCentroid() throws Exception {
    String mp = "POLYGON((-80 -10, -40 -10, -40 10, -80 10, -80 -10))";
    Polygon p = (Polygon) SimpleWKTShapeParser.parse(mp);
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    LatLonShapeDocValuesField dvField = LatLonShape.createDocValueField(FIELD_NAME, tess);
    assertEquals(0d, dvField.getCentroid().getLat(), 1E-7);
    assertEquals(-60.0, dvField.getCentroid().getLon(), 1E-7);
    assertEquals(TYPE.TRIANGLE, dvField.getHighestDimensionType());
  }

  /**
   * ensures consistency between {@link ByteBuffersDataOutput#writeVInt(int)} and {@link
   * ShapeDocValues#vIntSize(int)} and {@link ByteBuffersDataOutput#writeVLong(long)} and {@link
   * ShapeDocValues#vLongSize(long)} so the serialization is valid.
   */
  public void testVariableValueSizes() throws Exception {
    // scratch buffer
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();

    for (int i = 0; i < random().nextInt(100, 500); ++i) {
      // test variable int sizes
      int testInt = random().nextInt(Integer.MAX_VALUE);
      long pB = out.size();
      out.writeVInt(testInt);
      long pA = out.size();
      assertEquals(ShapeDocValues.vIntSize(testInt), (int) (pA - pB));

      // test variable long sizes
      long testLong = random().nextLong(Long.MAX_VALUE);
      out.writeVLong(testLong);
      assertEquals(ShapeDocValues.vLongSize(testLong), out.size() - pA);
    }
  }

  private Polygon getTestPolygonWithHole() {
    Polygon poly = GeoTestUtil.createRegularPolygon(0.0, 0.0, 100000, 7);
    Polygon inner =
        new Polygon(
            new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
            new double[] {1.0, -1.0, -0.5, -1.0, 1.0, 0.5, 1.0});
    Polygon inner2 =
        new Polygon(
            new double[] {-1.0, -1.0, 0.5, 1.0, 1.0, 0.5, -1.0},
            new double[] {-2.0, -4.0, -3.5, -4.0, -2.0, -2.5, -2.0});

    return new Polygon(poly.getPolyLats(), poly.getPolyLons(), inner, inner2);
  }

  private List<ShapeField.DecodedTriangle> getTessellation(Geometry p) {
    if (p instanceof Polygon) {
      return getTessellation((Polygon) p);
    } else if (p instanceof XYPolygon) {
      return getTessellation((XYPolygon) p);
    }
    throw new IllegalArgumentException("invalid geometry type: " + p.getClass());
  }

  private List<ShapeField.DecodedTriangle> getTessellation(Polygon p) {
    Field[] fields = LatLonShape.createIndexableFields(FIELD_NAME, p);
    List<ShapeField.DecodedTriangle> tess = new ArrayList<>(fields.length);
    for (Field f : fields) {
      ShapeField.DecodedTriangle d = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(f.binaryValue().bytes, d);
      tess.add(d);
    }
    return tess;
  }

  private List<ShapeField.DecodedTriangle> getTessellation(XYPolygon p) {
    Field[] fields = XYShape.createIndexableFields(FIELD_NAME, p, true);
    List<ShapeField.DecodedTriangle> tess = new ArrayList<>(fields.length);
    for (Field f : fields) {
      ShapeField.DecodedTriangle d = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(f.binaryValue().bytes, d);
      tess.add(d);
    }
    return tess;
  }

  /** Compute signed area of rectangle */
  private static double area(Polygon p) {
    return (p.maxLon - p.minLon) * (p.maxLat - p.minLat);
  }
}
