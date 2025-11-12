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

import static org.apache.lucene.geo.GeoUtils.orient;

import org.apache.lucene.index.PointValues;

/**
 * Interface for 2D geometric components that support spatial relationship queries.
 *
 * <p>Component2D defines the contract for testing spatial relationships between geometric shapes
 * and index structures (points, lines, triangles, bounding boxes). This interface is central to
 * Lucene's spatial search implementation, enabling filtering of documents during geospatial
 * queries.
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Point containment testing
 *   <li>Line and triangle intersection detection
 *   <li>Line and triangle containment testing
 *   <li>Bounding box relationship computation
 *   <li>"Within" relationship evaluation for spatial queries
 * </ul>
 *
 * <p>Implementations:
 *
 * <p>Component2D is implemented by geometric shapes classes:
 *
 * <ul>
 *   <li>{@link Circle2D} - Circular regions (both geographic and Cartesian)
 *   <li>{@link Polygon2D} - Polygonal regions with hole support
 *   <li>{@link Rectangle2D} - Rectangular bounding boxes
 *   <li>{@link Line2D} - Line segments and polylines
 *   <li>{@link Point2D} - Point geometries
 * </ul>
 *
 * <p>Usage:
 *
 * <p>Component2D instances are created by calling {@code toComponent2D()} on high-level geometry
 * objects:
 *
 * <pre>{@code
 * // Create from a Circle
 * double nycLat = 40.7128;
 * double nycLon = -74.0060;
 * double radiusMeters = 5000; // 5km radius
 * Circle circle = new Circle(nycLat, nycLon, radiusMeters);
 * Component2D component = circle.toComponent2D();
 *
 * // Test if a point is contained
 * double xCoordinate = -74.0060;
 * double yCoordinate = 40.7128;
 * boolean contained = component.contains(xCoordinate, yCoordinate);
 *
 * // Test relationship with a bounding box
 * double minX = -74.01;
 * double maxX = -74.00;
 * double minY = 40.71;
 * double maxY = 40.72;
 * PointValues.Relation relation = component.relate(minX, maxX, minY, maxY);
 * }</pre>
 *
 * <p>Spatial Relationship Methods:
 *
 * <p><strong>Containment:</strong>
 *
 * <ul>
 *   <li>{@link #contains(double, double)} - Tests if a point is inside the geometry
 *   <li>{@link #containsLine} - Tests if a line segment is fully contained
 *   <li>{@link #containsTriangle} - Tests if a triangle is fully contained
 * </ul>
 *
 * <p><strong>Intersection:</strong>
 *
 * <ul>
 *   <li>{@link #intersectsLine} - Tests if a line segment intersects the geometry
 *   <li>{@link #intersectsTriangle} - Tests if a triangle intersects the geometry
 * </ul>
 *
 * <p><strong>Relationship:</strong>
 *
 * <ul>
 *   <li>{@link #relate} - Determines spatial relationship with a bounding box (INSIDE, OUTSIDE, or
 *       CROSSES)
 *   <li>{@link #withinPoint} - Computes "within" relationship for a point
 *   <li>{@link #withinLine} - Computes "within" relationship for a line
 *   <li>{@link #withinTriangle} - Computes "within" relationship for a triangle
 * </ul>
 *
 * <p>Bounding Box:
 *
 * <p>Every Component2D has an associated bounding box defined by:
 *
 * <ul>
 *   <li>{@link #getMinX()}, {@link #getMaxX()} - Horizontal bounds
 *   <li>{@link #getMinY()}, {@link #getMaxY()} - Vertical bounds
 * </ul>
 *
 * <p><strong>Within Relationship:</strong>
 *
 * <p>The "within" relationship is used to determine if the query shape is contained within an
 * indexed shape. The {@link WithinRelation} enum provides three possible outcomes:
 *
 * <ul>
 *   <li>{@link WithinRelation#CANDIDATE} - The shape might be within (requires further validation)
 *   <li>{@link WithinRelation#NOTWITHIN} - The shape is definitely not within
 *   <li>{@link WithinRelation#DISJOINT} - The shapes don't intersect at all
 * </ul>
 *
 * <p>Performance Considerations:
 *
 * <ul>
 *   <li>Bounding box checks ({@link #getMinX()}, etc.) enable elimination of non-intersecting
 *       geometries
 *   <li>Methods accepting pre-computed bounding boxes (minX, maxX, minY, maxY) avoid redundant
 *       calculations
 *   <li>Default methods compute bounding boxes automatically for convenience
 *   <li>Implementations should optimize for the most common query patterns in their domain
 * </ul>
 *
 * <p>Coordinate Systems:
 *
 * <p>Component2D works with encoded coordinate values:
 *
 * <ul>
 *   <li><strong>Geographic</strong> - X represents longitude, Y represents latitude (both encoded)
 *   <li><strong>Cartesian</strong> - X and Y represent planar coordinates
 * </ul>
 *
 * <p>Utility Methods:
 *
 * <p>Component2D provides several static utility methods for common spatial operations:
 *
 * <ul>
 *   <li>{@link #disjoint} - Tests if two bounding boxes don't overlap
 *   <li>{@link #within} - Tests if one bounding box is fully contained within another
 *   <li>{@link #containsPoint} - Tests if a point is inside a rectangle
 *   <li>{@link #pointInTriangle} - Tests if a point is inside a triangle using winding order
 * </ul>
 *
 * @lucene.internal
 * @see Circle2D
 * @see Polygon2D
 * @see LatLonGeometry
 * @see XYGeometry
 */
public interface Component2D {

  /** min X value for the component * */
  double getMinX();

  /** max X value for the component * */
  double getMaxX();

  /** min Y value for the component * */
  double getMinY();

  /** max Y value for the component * */
  double getMaxY();

  /** relates this component2D with a point * */
  boolean contains(double x, double y);

  /** relates this component2D with a bounding box * */
  PointValues.Relation relate(double minX, double maxX, double minY, double maxY);

  /** return true if this component2D intersects the provided line * */
  boolean intersectsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY);

  /** return true if this component2D intersects the provided triangle * */
  boolean intersectsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY);

  /** return true if this component2D contains the provided line * */
  boolean containsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY);

  /** return true if this component2D contains the provided triangle * */
  boolean containsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY);

  /**
   * Used by withinTriangle to check the within relationship between a triangle and the query shape
   * (e.g. if the query shape is within the triangle).
   */
  enum WithinRelation {
    /**
     * If the shape is a candidate for within. Typically this is return if the query shape is fully
     * inside the triangle or if the query shape intersects only edges that do not belong to the
     * original shape.
     */
    CANDIDATE,
    /**
     * The query shape intersects an edge that does belong to the original shape or any point of the
     * triangle is inside the shape.
     */
    NOTWITHIN,
    /** The query shape is disjoint with the triangle. */
    DISJOINT
  }

  /** Compute the within relation of this component2D with a point * */
  WithinRelation withinPoint(double x, double y);

  /** Compute the within relation of this component2D with a line * */
  WithinRelation withinLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY);

  /** Compute the within relation of this component2D with a triangle * */
  WithinRelation withinTriangle(
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
      boolean ca);

  /** return true if this component2D intersects the provided line * */
  default boolean intersectsLine(double aX, double aY, double bX, double bY) {
    double minY = StrictMath.min(aY, bY);
    double minX = StrictMath.min(aX, bX);
    double maxY = StrictMath.max(aY, bY);
    double maxX = StrictMath.max(aX, bX);
    return intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY);
  }

  /** return true if this component2D intersects the provided triangle * */
  default boolean intersectsTriangle(
      double aX, double aY, double bX, double bY, double cX, double cY) {
    double minY = StrictMath.min(StrictMath.min(aY, bY), cY);
    double minX = StrictMath.min(StrictMath.min(aX, bX), cX);
    double maxY = StrictMath.max(StrictMath.max(aY, bY), cY);
    double maxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    return intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  /** return true if this component2D contains the provided line * */
  default boolean containsLine(double aX, double aY, double bX, double bY) {
    double minY = StrictMath.min(aY, bY);
    double minX = StrictMath.min(aX, bX);
    double maxY = StrictMath.max(aY, bY);
    double maxX = StrictMath.max(aX, bX);
    return containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY);
  }

  /** return true if this component2D contains the provided triangle * */
  default boolean containsTriangle(
      double aX, double aY, double bX, double bY, double cX, double cY) {
    double minY = StrictMath.min(StrictMath.min(aY, bY), cY);
    double minX = StrictMath.min(StrictMath.min(aX, bX), cX);
    double maxY = StrictMath.max(StrictMath.max(aY, bY), cY);
    double maxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    return containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  /** Compute the within relation of this component2D with a triangle * */
  default WithinRelation withinLine(double aX, double aY, boolean ab, double bX, double bY) {
    double minY = StrictMath.min(aY, bY);
    double minX = StrictMath.min(aX, bX);
    double maxY = StrictMath.max(aY, bY);
    double maxX = StrictMath.max(aX, bX);
    return withinLine(minX, maxX, minY, maxY, aX, aY, ab, bX, bY);
  }

  /** Compute the within relation of this component2D with a triangle * */
  default WithinRelation withinTriangle(
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY,
      boolean bc,
      double cX,
      double cY,
      boolean ca) {
    double minY = StrictMath.min(StrictMath.min(aY, bY), cY);
    double minX = StrictMath.min(StrictMath.min(aX, bX), cX);
    double maxY = StrictMath.max(StrictMath.max(aY, bY), cY);
    double maxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    return withinTriangle(minX, maxX, minY, maxY, aX, aY, ab, bX, bY, bc, cX, cY, ca);
  }

  /** Compute whether the bounding boxes are disjoint * */
  static boolean disjoint(
      double minX1,
      double maxX1,
      double minY1,
      double maxY1,
      double minX2,
      double maxX2,
      double minY2,
      double maxY2) {
    return (maxY1 < minY2 || minY1 > maxY2 || maxX1 < minX2 || minX1 > maxX2);
  }

  /** Compute whether the first bounding box 1 is within the second bounding box * */
  static boolean within(
      double minX1,
      double maxX1,
      double minY1,
      double maxY1,
      double minX2,
      double maxX2,
      double minY2,
      double maxY2) {
    return (minY2 <= minY1 && maxY2 >= maxY1 && minX2 <= minX1 && maxX2 >= maxX1);
  }

  /** returns true if rectangle (defined by minX, maxX, minY, maxY) contains the X Y point */
  static boolean containsPoint(
      final double x,
      final double y,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY) {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  /** Compute whether the given x, y point is in a triangle; uses the winding order method */
  static boolean pointInTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double x,
      double y,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY) {
    // check the bounding box because if the triangle is degenerated, e.g points and lines, we need
    // to filter out
    // coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
      int a = orient(x, y, aX, aY, bX, bY);
      int b = orient(x, y, bX, bY, cX, cY);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cX, cY, aX, aY);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
  }
}
