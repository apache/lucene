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
package org.apache.lucene.geo;

import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

import java.util.Objects;
import org.apache.lucene.index.PointValues;

/**
 * Internal 2D representation of a rectangle for spatial query operations.
 *
 * <p>This class provides spatial logic for rectangle-based queries, converting high-level {@link
 * Rectangle} or {@link XYRectangle} geometries into 2D representations. It is used internally by
 * Lucene's spatial search implementation for bounding box operations.
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Point containment checks using simple coordinate comparisons
 *   <li>Bounding box relationship calculations
 *   <li>Line and triangle intersection detection
 *   <li>Support for both geographic (lat/lon) and Cartesian coordinate systems
 *   <li>Handling of dateline-crossing rectangles
 * </ul>
 *
 * <p>Coordinate Systems:
 *
 * <ul>
 *   <li><strong>Geographic (lat/lon)</strong> - X represents longitude, Y represents latitude (both
 *       encoded as integers)
 *   <li><strong>Cartesian (XY)</strong> - X and Y represent planar coordinates
 * </ul>
 *
 * <p>Usage:
 *
 * <p>Rectangle2D instances are created through the static {@link #create(Rectangle)} or {@link
 * #create(XYRectangle)} factory methods:
 *
 * <pre>{@code
 * // Geographic rectangle (lat/lon)
 * double minLat = 40.7;
 * double maxLat = 40.8;
 * double minLon = -74.0;
 * double maxLon = -73.9;
 * Rectangle geoRect = new Rectangle(minLat, maxLat, minLon, maxLon);
 * Component2D geoComponent = Rectangle2D.create(geoRect);
 *
 * // Query operations
 * double testLon = -73.95;
 * double testLat = 40.75;
 * boolean contained = geoComponent.contains(testLon, testLat);
 *
 * // Cartesian rectangle (XY)
 * float minX = 100.0f;
 * float maxX = 200.0f;
 * float minY = 150.0f;
 * float maxY = 250.0f;
 * XYRectangle xyRect = new XYRectangle(minX, maxX, minY, maxY);
 * Component2D xyComponent = Rectangle2D.create(xyRect);
 *
 * }</pre>
 *
 * <p>Dateline Crossing:
 *
 * <p>For geographic rectangles that cross the International Date Line (where minLon &gt; maxLon),
 * Rectangle2D automatically creates a {@link ComponentTree} containing two separate Rectangle2D
 * components to handle the split geometry correctly:
 *
 * <pre>{@code
 * // Rectangle crossing the dateline (170°E to -170°E)
 * double minLat = 20.0;
 * double maxLat = 30.0;
 * double minLon = 170.0;  // East of dateline
 * double maxLon = -170.0; // West of dateline
 * Rectangle datelineRect = new Rectangle(minLat, maxLat, minLon, maxLon);
 *
 * // Creates ComponentTree with two rectangles internally:
 * // 1. From 170°E to 180°E
 * // 2. From -180°E to -170°E
 * Component2D component = Rectangle2D.create(datelineRect);
 * }</pre>
 *
 * <p>Performance Characteristics:
 *
 * <ul>
 *   <li><strong>Point containment</strong> - O(1) using simple coordinate comparisons
 *   <li><strong>Bounding box checks</strong> - O(1) for disjoint and containment tests
 *   <li><strong>Line intersection</strong> - O(1) checking against 4 edges
 *   <li><strong>Triangle intersection</strong> - O(1) checking vertices and edges
 * </ul>
 *
 * <p>Spatial Operations:
 *
 * <p>Rectangle2D implements all {@link Component2D} operations:
 *
 * <ul>
 *   <li>{@link #contains(double, double)} - Tests if a point is inside the rectangle
 *   <li>{@link #relate(double, double, double, double)} - Determines relationship with a bounding
 *       box
 *   <li>{@link #intersectsLine} - Tests if a line segment intersects the rectangle
 *   <li>{@link #intersectsTriangle} - Tests if a triangle intersects the rectangle
 *   <li>{@link #containsLine} - Tests if a line is fully contained (checks bounding boxes)
 *   <li>{@link #containsTriangle} - Tests if a triangle is fully contained (checks bounding boxes)
 * </ul>
 *
 * <p>Edge Intersection Algorithm:
 *
 * <p>Rectangle2D uses the {@link #edgesIntersect(double, double, double, double)} method to check
 * if a line segment intersects any of the rectangle's four edges. This method:
 *
 * <ol>
 *   <li>First performs a quick bounding box check to eliminate obvious non-intersections
 *   <li>Tests intersection against each of the four rectangle edges (top, right, bottom, left)
 *   <li>Uses {@link GeoUtils#lineCrossesLineWithBoundary} for precise intersection detection
 * </ol>
 *
 * <p>Coordinate Encoding:
 *
 * <p>For geographic rectangles, latitude and longitude values are quantized to match Lucene's point
 * encoding:
 *
 * <ul>
 *   <li>Minimum values use ceiling encoding ({@code encodeLatitudeCeil}, {@code
 *       encodeLongitudeCeil})
 *   <li>Maximum values use standard encoding ({@code encodeLatitude}, {@code encodeLongitude})
 *   <li>This ensures the encoded rectangle fully contains the original coordinate space
 * </ul>
 *
 * <p>Use Cases:
 *
 * <ul>
 *   <li>Bounding box queries in spatial indexes
 *   <li>Range queries on lat/lon or XY coordinates
 *   <li>Quick spatial filtering before more expensive geometry tests
 *   <li>Building larger spatial structures (e.g., R-trees)
 * </ul>
 *
 * @lucene.internal
 * @see Rectangle
 * @see XYRectangle
 * @see Component2D
 * @see ComponentTree
 */
final class Rectangle2D implements Component2D {

  private final double minX;
  private final double maxX;
  private final double minY;
  private final double maxY;

  private Rectangle2D(double minX, double maxX, double minY, double maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }

  @Override
  public double getMinX() {
    return minX;
  }

  @Override
  public double getMaxX() {
    return maxX;
  }

  @Override
  public double getMinY() {
    return minY;
  }

  @Override
  public double getMaxY() {
    return maxY;
  }

  @Override
  public boolean contains(double x, double y) {
    return Component2D.containsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY)) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public boolean intersectsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) || contains(bX, bY) || edgesIntersect(aX, aY, bX, bY);
  }

  @Override
  public boolean intersectsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY)
        || contains(bX, bY)
        || contains(cX, cY)
        || Component2D.pointInTriangle(
            minX, maxX, minY, maxY, this.minX, this.minY, aX, aY, bX, bY, cX, cY)
        || edgesIntersect(aX, aY, bX, bY)
        || edgesIntersect(bX, bY, cX, cY)
        || edgesIntersect(cX, cY, aX, aY);
  }

  @Override
  public boolean containsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY) {
    return Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public boolean containsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY) {
    return Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }
    if (contains(aX, aY) || contains(bX, bY)) {
      return WithinRelation.NOTWITHIN;
    }
    if (ab == true && edgesIntersect(aX, aY, bX, bY)) {
      return WithinRelation.NOTWITHIN;
    }
    return WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY,
      boolean bc,
      double cX,
      double cY,
      boolean ca) {
    // Bounding boxes disjoint?
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // Points belong to the shape so if points are inside the rectangle then it cannot be within.
    if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
      return WithinRelation.NOTWITHIN;
    }
    // If any of the edges intersects an edge belonging to the shape then it cannot be within.
    WithinRelation relation = WithinRelation.DISJOINT;
    if (edgesIntersect(aX, aY, bX, bY) == true) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (edgesIntersect(bX, bY, cX, cY) == true) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (edgesIntersect(cX, cY, aX, aY) == true) {
      if (ca == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    // If any of the rectangle edges crosses a triangle edge that does not belong to the shape
    // then it is a candidate for within
    if (relation == WithinRelation.CANDIDATE) {
      return WithinRelation.CANDIDATE;
    }
    // Check if shape is within the triangle
    if (Component2D.pointInTriangle(
        minX, maxX, minY, maxY, this.minX, this.minY, aX, aY, bX, bY, cX, cY)) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  private boolean edgesIntersect(double aX, double aY, double bX, double bY) {
    // shortcut: check bboxes of edges are disjoint
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Rectangle2D)) return false;
    Rectangle2D that = (Rectangle2D) o;
    return Double.compare(minX, that.minX) == 0
        && Double.compare(maxX, that.maxX) == 0
        && Double.compare(minY, that.minY) == 0
        && Double.compare(maxY, that.maxY) == 0;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(minX, maxX, minY, maxY);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Rectangle2D(x=");
    sb.append(minX);
    sb.append(" TO ");
    sb.append(maxX);
    sb.append(" y=");
    sb.append(minY);
    sb.append(" TO ");
    sb.append(maxY);
    sb.append(")");
    return sb.toString();
  }

  /** create a component2D from the provided XY rectangle */
  static Component2D create(XYRectangle rectangle) {
    return new Rectangle2D(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY);
  }

  private static final double MIN_LON_INCL_QUANTIZE = decodeLongitude(MIN_LON_ENCODED);
  private static final double MAX_LON_INCL_QUANTIZE = decodeLongitude(MAX_LON_ENCODED);

  /** create a component2D from the provided LatLon rectangle */
  static Component2D create(Rectangle rectangle) {
    // behavior of LatLonPoint.newBoxQuery()
    double minLongitude = rectangle.minLon;
    boolean crossesDateline = rectangle.minLon > rectangle.maxLon;
    if (minLongitude == 180.0 && crossesDateline) {
      minLongitude = -180;
      crossesDateline = false;
    }
    // need to quantize!
    double qMinLat = decodeLatitude(encodeLatitudeCeil(rectangle.minLat));
    double qMaxLat = decodeLatitude(encodeLatitude(rectangle.maxLat));
    double qMinLon = decodeLongitude(encodeLongitudeCeil(minLongitude));
    double qMaxLon = decodeLongitude(encodeLongitude(rectangle.maxLon));
    if (crossesDateline) {
      // for rectangles that cross the dateline we need to create two components
      Component2D[] components = new Component2D[2];
      components[0] = new Rectangle2D(MIN_LON_INCL_QUANTIZE, qMaxLon, qMinLat, qMaxLat);
      components[1] = new Rectangle2D(qMinLon, MAX_LON_INCL_QUANTIZE, qMinLat, qMaxLat);
      return ComponentTree.create(components);
    } else {
      return new Rectangle2D(qMinLon, qMaxLon, qMinLat, qMaxLat);
    }
  }
}
