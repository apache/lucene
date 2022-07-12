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
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Simple tests for {@link org.apache.lucene.document.ShapeDocValuesField} */
public class TestShapeDocValues extends LuceneTestCase {

  public void testSimpleDocValueField() throws Exception {
    List<ShapeField.DecodedTriangle> t = getTessellation(getTestPolygonWithHole());
    ShapeDocValuesField dvField = new ShapeDocValuesField("field", t);
    assertEquals(
        dvField.relate(-21458726, -21456626, -10789313, -10629313),
        PointValues.Relation.CELL_OUTSIDE_QUERY);
    assertNotEquals(
        dvField.relate(21456626, 21458726, 10629313, 10789313),
        PointValues.Relation.CELL_CROSSES_QUERY);
  }

  public void testLatLonPolygonCentroid() {
    Polygon p = GeoTestUtil.nextPolygon();
    Point expected = (Point) computeCentroid(p);
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    ShapeDocValuesField dvField = new ShapeDocValuesField("field", tess);
    assertEquals(tess.size(), dvField.numberOfTerms());
    assertEquals(expected.getLat(), GeoEncodingUtils.decodeLatitude(dvField.getCentroidY()), 1E-8);
    assertEquals(expected.getLon(), GeoEncodingUtils.decodeLongitude(dvField.getCentroidX()), 1E-8);
    assertEquals(ShapeField.DecodedTriangle.TYPE.TRIANGLE, dvField.getHighestDimensionType());
  }

  public void testXYPolygonCentroid() {
    XYPolygon p = (XYPolygon) BaseXYShapeTestCase.ShapeType.POLYGON.nextShape();
    XYPoint expected = (XYPoint) computeCentroid(p);
    ShapeDocValuesField dvField = new ShapeDocValuesField("field", getTessellation(p));
    assertEquals(expected.getX(), XYEncodingUtils.decode(dvField.getCentroidX()), 1E-8);
    assertEquals(expected.getY(), XYEncodingUtils.decode(dvField.getCentroidY()), 1E-8);
    assertEquals(ShapeField.DecodedTriangle.TYPE.TRIANGLE, dvField.getHighestDimensionType());
  }

  private Geometry computeCentroid(Geometry p) {
    List<ShapeField.DecodedTriangle> tess = getTessellation(p);
    double totalSignedArea = 0;
    double numXPly = 0;
    double numYPly = 0;
    for (ShapeField.DecodedTriangle t : tess) {
      double signedArea =
          Math.abs(0.5d * ((t.bX - t.aX) * (t.cY - t.aY) - (t.cX - t.aX) * (t.bY - t.aY)));
      // accumulate midPoints and signed area
      numXPly += (((t.aX + t.bX + t.cX) / 3) * signedArea);
      numYPly += (((t.aY + t.bY + t.cY) / 3) * signedArea);
      totalSignedArea += signedArea;
    }

    if (p instanceof Polygon) {
      return new Point(
          GeoEncodingUtils.decodeLatitude((int) (numYPly / totalSignedArea)),
          GeoEncodingUtils.decodeLongitude((int) (numXPly / totalSignedArea)));
    } else if (p instanceof XYPolygon) {
      return new XYPoint(
          XYEncodingUtils.decode((int) (numXPly / totalSignedArea)),
          XYEncodingUtils.decode((int) (numYPly / totalSignedArea)));
    }
    throw new IllegalArgumentException("invalid geometry type: " + p.getClass());
  }

  /**
   * ensures consistency between {@link ByteBuffersDataOutput#writeVInt(int)} and {@link
   * ShapeDocValuesField#vIntSize(int)} and {@link ByteBuffersDataOutput#writeVLong(long)} and
   * {@link ShapeDocValuesField#vLongSize(long)} so the serialization is valid.
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
      assertEquals(ShapeDocValuesField.vIntSize(testInt), (int) (pA - pB));

      // test variable long sizes
      long testLong = random().nextLong(Long.MAX_VALUE);
      out.writeVLong(testLong);
      assertEquals(ShapeDocValuesField.vLongSize(testLong), out.size() - pA);
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
    Field[] fields = LatLonShape.createIndexableFields("test", p);
    List<ShapeField.DecodedTriangle> tess = new ArrayList<>(fields.length);
    for (Field f : fields) {
      ShapeField.DecodedTriangle d = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(f.binaryValue().bytes, d);
      tess.add(d);
    }
    return tess;
  }

  private List<ShapeField.DecodedTriangle> getTessellation(XYPolygon p) {
    Field[] fields = XYShape.createIndexableFields("test", p, true);
    List<ShapeField.DecodedTriangle> tess = new ArrayList<>(fields.length);
    for (Field f : fields) {
      ShapeField.DecodedTriangle d = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(f.binaryValue().bytes, d);
      tess.add(d);
    }
    return tess;
  }
}
