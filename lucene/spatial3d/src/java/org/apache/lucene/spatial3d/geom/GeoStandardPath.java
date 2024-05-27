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
package org.apache.lucene.spatial3d.geom;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.internal.hppc.IntArrayList;

/**
 * GeoShape representing a path across the surface of the globe, with a specified half-width. Path
 * is described by a series of points. Distances are measured from the starting point along the
 * path, and then at right angles to the path.
 *
 * @lucene.internal
 */
class GeoStandardPath extends GeoBasePath {
  /** The cutoff angle (width) */
  protected final double cutoffAngle;

  /** Sine of cutoff angle */
  protected final double sinAngle;

  /** Cosine of cutoff angle */
  protected final double cosAngle;

  /** The original list of path points */
  protected final List<GeoPoint> points = new ArrayList<GeoPoint>();

  /** The b-tree of PathComponents */
  protected PathComponent rootComponent;

  /** A point on the edge */
  protected GeoPoint[] edgePoints;

  /** Set to true if path has been completely constructed */
  protected boolean isDone = false;

  /**
   * Constructor.
   *
   * @param planetModel is the planet model.
   * @param maxCutoffAngle is the width of the path, measured as an angle.
   * @param pathPoints are the points in the path.
   */
  public GeoStandardPath(
      final PlanetModel planetModel, final double maxCutoffAngle, final GeoPoint[] pathPoints) {
    this(planetModel, maxCutoffAngle);
    Collections.addAll(points, pathPoints);
    done();
  }

  /**
   * Piece-wise constructor. Use in conjunction with addPoint() and done().
   *
   * @param planetModel is the planet model.
   * @param maxCutoffAngle is the width of the path, measured as an angle.
   */
  public GeoStandardPath(final PlanetModel planetModel, final double maxCutoffAngle) {
    super(planetModel);
    if (maxCutoffAngle <= 0.0 || maxCutoffAngle > Math.PI * 0.5) {
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    }
    this.cutoffAngle = maxCutoffAngle;
    this.cosAngle = Math.cos(maxCutoffAngle);
    this.sinAngle = Math.sin(maxCutoffAngle);
  }

  /**
   * Add a point to the path.
   *
   * @param lat is the latitude of the point.
   * @param lon is the longitude of the point.
   */
  public void addPoint(final double lat, final double lon) {
    if (isDone) {
      throw new IllegalStateException("Can't call addPoint() if done() already called");
    }
    points.add(new GeoPoint(planetModel, lat, lon));
  }

  /** Complete the path. */
  public void done() {
    if (isDone) {
      throw new IllegalStateException("Can't call done() twice");
    }
    if (points.size() == 0) {
      throw new IllegalArgumentException("Path must have at least one point");
    }
    isDone = true;

    final List<SegmentEndpoint> endPoints = new ArrayList<>(points.size());
    final List<PathSegment> segments = new ArrayList<>(points.size());

    // Compute an offset to use for all segments.  This will be based on the minimum magnitude of
    // the entire ellipsoid.
    final double cutoffOffset = this.sinAngle * planetModel.getMinimumMagnitude();

    // The major output we want to produce here is a balanced binary tree of PathComponents.
    // This code first produces a list of path segments and segment endpoints, and then we do
    // the following:
    // (0) Initialize a "stack", which contains ordered path components
    // (1) Walk through the path components in order
    // (2) Add the next path component to the stack
    // (3) If there are two path components of the same depth, remove them and add a PathNode
    //     with them as children
    // (4) At the end, if there are more than one thing on the stack, build a final PathNode
    //     with those two
    // (5) Use the top element as the root.
    // I've structured this as its own class that just receives path components in order,
    // and returns the final one at the end.

    // First, build all segments.  We'll then go back and build corresponding segment endpoints.
    GeoPoint lastPoint = null;
    PathComponent lastComponent = null;
    for (final GeoPoint end : points) {
      if (lastPoint != null) {
        final Plane normalizedConnectingPlane = new Plane(lastPoint, end);
        final PathSegment newComponent =
            new PathSegment(
                planetModel,
                lastComponent,
                lastPoint,
                end,
                normalizedConnectingPlane,
                cutoffOffset);
        segments.add(newComponent);
        lastComponent = newComponent;
      }
      lastPoint = end;
    }

    if (segments.size() == 0) {
      // Simple circle
      double lat = points.get(0).getLatitude();
      double lon = points.get(0).getLongitude();
      // Compute two points on the circle, with the right angle from the center.  We'll use these
      // to obtain the perpendicular plane to the circle.
      double upperLat = lat + cutoffAngle;
      double upperLon = lon;
      if (upperLat > Math.PI * 0.5) {
        upperLon += Math.PI;
        if (upperLon > Math.PI) {
          upperLon -= 2.0 * Math.PI;
        }
        upperLat = Math.PI - upperLat;
      }
      double lowerLat = lat - cutoffAngle;
      double lowerLon = lon;
      if (lowerLat < -Math.PI * 0.5) {
        lowerLon += Math.PI;
        if (lowerLon > Math.PI) {
          lowerLon -= 2.0 * Math.PI;
        }
        lowerLat = -Math.PI - lowerLat;
      }
      final GeoPoint upperPoint = new GeoPoint(planetModel, upperLat, upperLon);
      final GeoPoint lowerPoint = new GeoPoint(planetModel, lowerLat, lowerLon);
      final GeoPoint point = points.get(0);

      // Construct normal plane
      // TBD - what does this actually do?  Why Z??
      final Plane normalPlane = Plane.constructNormalizedZPlane(upperPoint, lowerPoint, point);

      final CircleSegmentEndpoint onlyEndpoint =
          new CircleSegmentEndpoint(planetModel, null, point, normalPlane, upperPoint, lowerPoint);
      endPoints.add(onlyEndpoint);
      this.edgePoints =
          new GeoPoint[] {
            onlyEndpoint.circlePlane.getSampleIntersectionPoint(planetModel, normalPlane)
          };
    } else {
      // Create segment endpoints.  Use an appropriate constructor for the start and end of the
      // path.
      for (int i = 0; i < segments.size(); i++) {
        final PathSegment currentSegment = segments.get(i);

        if (i == 0) {
          // Starting endpoint. The cutoff plane we use is the start cutoff plane from the first
          // segment, and the point involved is the start point.
          final SegmentEndpoint startEndpoint =
              new CutoffSingleCircleSegmentEndpoint(
                  planetModel,
                  null,
                  currentSegment.start,
                  currentSegment.startCutoffPlane,
                  currentSegment.ULHC,
                  currentSegment.LLHC);
          endPoints.add(startEndpoint);
          this.edgePoints = new GeoPoint[] {currentSegment.ULHC};
          continue;
        }

        // General intersection case.
        // The CutoffDualCircleSegmentEndpoint is advanced enough now to handle
        // joinings that are perfectly in a line, I believe, so I am disabling
        // the special code for that situation.
        final PathSegment prevSegment = segments.get(i - 1);
        endPoints.add(
            new CutoffDualCircleSegmentEndpoint(
                planetModel,
                prevSegment,
                currentSegment.start,
                prevSegment.endCutoffPlane,
                currentSegment.startCutoffPlane,
                prevSegment.URHC,
                prevSegment.LRHC,
                currentSegment.ULHC,
                currentSegment.LLHC));
        // }
      }
      // Do final endpoint. Cutoff plane is the end cutoff plane from the last path segment.
      // The final endpoint is the last segment's endpoint.
      final PathSegment lastSegment = segments.get(segments.size() - 1);
      endPoints.add(
          new CutoffSingleCircleSegmentEndpoint(
              planetModel,
              lastSegment,
              lastSegment.end,
              lastSegment.endCutoffPlane,
              lastSegment.URHC,
              lastSegment.LRHC));
    }

    final TreeBuilder treeBuilder = new TreeBuilder(segments.size() + endPoints.size());
    // Segments will have one less than the number of endpoints.
    // So, we add the first endpoint, and then do it pairwise.
    treeBuilder.addComponent(endPoints.get(0));
    for (int i = 0; i < segments.size(); i++) {
      treeBuilder.addComponent(segments.get(i));
      treeBuilder.addComponent(endPoints.get(i + 1));
    }

    rootComponent = treeBuilder.getRoot();

    // System.out.println("Root component: "+rootComponent);
  }

  /**
   * Constructor for deserialization.
   *
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoStandardPath(final PlanetModel planetModel, final InputStream inputStream)
      throws IOException {
    this(
        planetModel,
        SerializableObject.readDouble(inputStream),
        SerializableObject.readPointArray(planetModel, inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, cutoffAngle);
    SerializableObject.writePointArray(outputStream, points);
  }

  @Override
  public double computePathCenterDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (rootComponent == null) {
      return Double.POSITIVE_INFINITY;
    }
    return distanceStyle.fromAggregationForm(
        rootComponent.pathCenterDistance(distanceStyle, x, y, z));
  }

  @Override
  public double computeNearestDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (rootComponent == null) {
      return Double.POSITIVE_INFINITY;
    }
    final DistancePair distancePair = rootComponent.nearestDistance(distanceStyle, x, y, z);
    if (distancePair == null) {
      return Double.POSITIVE_INFINITY;
    }
    return distanceStyle.fromAggregationForm(distancePair.distanceAlongPath);
  }

  @Override
  protected double distance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (rootComponent == null) {
      return Double.POSITIVE_INFINITY;
    }
    return distanceStyle.fromAggregationForm(rootComponent.distance(distanceStyle, x, y, z));
  }

  @Override
  protected double deltaDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (rootComponent == null) {
      return Double.POSITIVE_INFINITY;
    }
    return distanceStyle.fromAggregationForm(
        rootComponent.pathDeltaDistance(distanceStyle, x, y, z));
  }

  @Override
  protected void distanceBounds(
      final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    // TBD: Compute actual bounds based on distance
    getBounds(bounds);
  }

  @Override
  protected double outsideDistance(
      final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (rootComponent == null) {
      return Double.POSITIVE_INFINITY;
    }
    return distanceStyle.fromAggregationForm(rootComponent.outsideDistance(distanceStyle, x, y, z));
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    if (rootComponent == null) {
      return false;
    }
    return rootComponent.isWithin(x, y, z);
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(
      final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds) {
    // We look for an intersection with any of the exterior edges of the path.
    // We also have to look for intersections with the cones described by the endpoints.
    // Return "true" if any such intersections are found.

    // For plane intersections, the basic idea is to come up with an equation of the line that is
    // the intersection (if any).  Then, find the intersections with the unit sphere (if any).  If
    // any of the intersection points are within the bounds, then we've detected an intersection.
    // Well, sort of.  We can detect intersections also due to overlap of segments with each other.
    // But that's an edge case and we won't be optimizing for it.
    // System.err.println(" Looking for intersection of plane " + plane + " with path " + this);
    if (rootComponent == null) {
      return false;
    }
    // Optimization: compute plane bounds and use that in the b-tree to check if we need to do
    // an actual intersection check.  This is disabled for the moment because there are randomized
    // test failures that need research to figure out.
    final XYZBounds planeBounds = null;
    /*
    final XYZBounds planeBounds = new XYZBounds();
    if (notablePoints != null) {
      for (final GeoPoint notablePoint : notablePoints) {
        planeBounds.addPoint(notablePoint);
      }
    }
    planeBounds.addPlane(planetModel, plane, bounds);
    */
    return rootComponent.intersects(plane, planeBounds, notablePoints, bounds);
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    if (rootComponent == null) {
      return false;
    }
    return rootComponent.intersects(geoShape);
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    if (rootComponent != null) {
      rootComponent.getBounds(bounds);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoStandardPath)) {
      return false;
    }
    GeoStandardPath p = (GeoStandardPath) o;
    if (!super.equals(p)) {
      return false;
    }
    if (cutoffAngle != p.cutoffAngle) {
      return false;
    }
    return points.equals(p.points);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp = Double.doubleToLongBits(cutoffAngle);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + points.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoStandardPath: {planetmodel="
        + planetModel
        + ", width="
        + cutoffAngle
        + "("
        + cutoffAngle * 180.0 / Math.PI
        + "), points={"
        + points
        + "}}";
  }

  private static class DistancePair {
    public final double pathCenterDistance;
    public final double distanceAlongPath;

    public DistancePair(final double pathCenterDistance, final double distanceAlongPath) {
      this.pathCenterDistance = pathCenterDistance;
      this.distanceAlongPath = distanceAlongPath;
    }

    @Override
    public String toString() {
      return "DistancePair: pathCenterDistance="
          + pathCenterDistance
          + ",distanceAlongPath="
          + distanceAlongPath;
    }
  }

  /**
   * Path components consist of both path segments and segment endpoints. This interface links their
   * behavior without having to know anything else about them.
   */
  private interface PathComponent {
    /**
     * Check if point is within this endpoint.
     *
     * @param point is the point.
     * @return true of within.
     */
    boolean isWithin(final Vector point);

    /**
     * Check if point is within this endpoint.
     *
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return true of within.
     */
    boolean isWithin(final double x, final double y, final double z);

    /** Check if point is within this section (within cutoff planes). */
    boolean isWithinSection(final Vector point);

    /** Check if point is within this section (within cutoff planes). */
    boolean isWithinSection(final double x, final double y, final double z);

    /**
     * Retrieve the starting distance along the path for this path element.
     *
     * @param distanceStyle is the distance style
     * @return the starting path distance in aggregation form
     */
    double getStartingDistance(final DistanceStyle distanceStyle);

    /**
     * Compute the total distance this path component adds to the path.
     *
     * @param distanceStyle is the distance style.
     * @return the full path distance.
     */
    double fullPathDistance(final DistanceStyle distanceStyle);

    /**
     * Compute distance measure starting from beginning of the path and including perpendicular
     * dog-leg to a point within the corridor.
     *
     * @param distanceStyle is the distance style
     * @param x is the x coordinate of the point we want to get the distance to
     * @param y is the y coordinate of the point we want to get the distance to
     * @param z is the z coordinate of the point we want to get the distance to
     * @return the distance from start of path
     */
    double distance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Get the nearest distance for a point. This is the old "legacy" computation: We find the
     * segment endpoint or path segment with the closest pathCenterDistance(), and keep track of the
     * one where that's at a minimum. We then compute nearestPathDistance() if it's a segment and
     * add that to fullPathDistance() computed along the entire path up to that point.
     *
     * <p>So what we are minimizing is not what we are returning here. That is why this is tricky to
     * modularize; we need to return two values: the best pathCenterDistance, and the corresponding
     * nearestPathDistance + startingDistance.
     *
     * @param distanceStyle is the distance style
     * @param x is the x coordinate of the point we want to get the distance to
     * @param y is the y coordinate of the point we want to get the distance to
     * @param z is the z coordinate of the point we want to get the distance to
     * @return the DistancePair containing both distances described above
     */
    DistancePair nearestDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Compute path distance.
     *
     * @param distanceStyle is the distance style.
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return the distance
     */
    double pathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Compute delta path distance.
     *
     * @param distanceStyle is the distance style.
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return the distance metric, in aggregation form.
     */
    double pathDeltaDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Compute nearest path distance (distance from start of segment to point adjacent the one
     * specitied, if reachable by this segment).
     *
     * @param distanceStyle is the distance style.
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return the distance metric, in aggregation form.
     */
    double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Compute path center distance (distance from the point to center of the path, if reachable by
     * this segment).
     *
     * @param distanceStyle is the distance style.
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return the distance metric, or POSITIVE_INFINITY if the point is not within the bounds of
     *     the path segment.
     */
    double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Compute external distance.
     *
     * @param distanceStyle is the distance style.
     * @param x is the point x.
     * @param y is the point y.
     * @param z is the point z.
     * @return the distance metric.
     */
    double outsideDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /**
     * Determine if this endpoint intersects a specified plane.
     *
     * @param p is the plane.
     * @param planeBounds are the XYZBounds of the plane we're looking for an intersection with.
     * @param notablePoints are the points associated with the plane.
     * @param bounds are any bounds which the intersection must lie within.
     * @return true if there is a matching intersection.
     */
    boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds);

    /**
     * Determine if this endpoint intersects a GeoShape.
     *
     * @param geoShape is the GeoShape.
     * @return true if there is shape intersect this endpoint.
     */
    boolean intersects(final GeoShape geoShape);

    /**
     * Get the bounds for a segment endpoint.
     *
     * @param bounds are the bounds to be modified.
     */
    void getBounds(final Bounds bounds);
  }

  private static class PathNode implements PathComponent {
    protected final PathComponent child1;
    protected final PathComponent child2;

    protected final XYZBounds bounds;

    public PathNode(final PathComponent child1, final PathComponent child2) {
      this.child1 = child1;
      this.child2 = child2;
      bounds = new XYZBounds();
      child1.getBounds(bounds);
      child2.getBounds(bounds);
      // System.out.println("Constructed PathNode with child1="+child1+" and child2="+child2+" with
      // computed bounds "+bounds);
    }

    @Override
    public boolean isWithin(final Vector point) {
      return isWithin(point.x, point.y, point.z);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      // We computed the bounds for the node already, so use that as an "early-out".
      // If we don't leave early, we need to check both children.
      if (x < bounds.getMinimumX()
          || x > bounds.getMaximumX()
          || y < bounds.getMinimumY()
          || y > bounds.getMaximumY()
          || z < bounds.getMinimumZ()
          || z > bounds.getMaximumZ()) {
        // Debugging: check that this really is true.
        /*
        if (child1.isWithin(x, y, z) || child2.isWithin(x, y, z)) {
          System.out.println("XYZBounds of PathNode inconsistent with isWithin of children! XYZBounds="+bounds+" child1="+child1+" child2="+child2+" Point=["+x+", "+y+", "+z+"]");
          XYZBounds ch1Bounds = new XYZBounds();
          child1.getBounds(ch1Bounds);
          XYZBounds ch2Bounds = new XYZBounds();
          child2.getBounds(ch2Bounds);
          System.out.println("Child1: Bounds="+ch1Bounds+" isWithin="+child1.isWithin(x,y,z));
          System.out.println("Child2: Bounds="+ch2Bounds+" isWithin="+child2.isWithin(x,y,z));
        }
        */
        return false;
      }

      return child1.isWithin(x, y, z) || child2.isWithin(x, y, z);
    }

    @Override
    public boolean isWithinSection(final Vector point) {
      return child1.isWithinSection(point) || child2.isWithinSection(point);
    }

    @Override
    public boolean isWithinSection(final double x, final double y, final double z) {
      return child1.isWithinSection(x, y, z) || child2.isWithinSection(x, y, z);
    }

    @Override
    public double getStartingDistance(final DistanceStyle distanceStyle) {
      return child1.getStartingDistance(distanceStyle);
    }

    @Override
    public double distance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      final double child1Distance = child1.distance(distanceStyle, x, y, z);
      final double child2Distance = child2.distance(distanceStyle, x, y, z);
      return Math.min(child1Distance, child2Distance);
    }

    @Override
    public DistancePair nearestDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // Since there's no early-out for sections, this has no benefit and is actually a drawback
      // if (!isWithinSection(x, y, z)) {
      //   return null;
      // }
      final DistancePair firstChildDistance = child1.nearestDistance(distanceStyle, x, y, z);
      final DistancePair secondChildDistance = child2.nearestDistance(distanceStyle, x, y, z);

      if (firstChildDistance == null) {
        return secondChildDistance;
      } else if (secondChildDistance == null) {
        return firstChildDistance;
      }

      // Optimize for lowest pathCenterDistance, but if those are equal, optimize for distance along
      // path.
      if (firstChildDistance.pathCenterDistance < secondChildDistance.pathCenterDistance) {
        return firstChildDistance;
      } else if (secondChildDistance.pathCenterDistance < firstChildDistance.pathCenterDistance) {
        return secondChildDistance;
      } else if (firstChildDistance.distanceAlongPath < secondChildDistance.distanceAlongPath) {
        return firstChildDistance;
      } else {
        return secondChildDistance;
      }
    }

    @Override
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      return distanceStyle.aggregateDistances(
          child1.fullPathDistance(distanceStyle), child2.fullPathDistance(distanceStyle));
    }

    @Override
    public double pathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return Math.min(
          child1.pathDistance(distanceStyle, x, y, z),
          distanceStyle.aggregateDistances(
              child1.fullPathDistance(distanceStyle), child2.pathDistance(distanceStyle, x, y, z)));
    }

    @Override
    public double pathDeltaDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return Math.min(
          child1.pathDeltaDistance(distanceStyle, x, y, z),
          child2.pathDeltaDistance(distanceStyle, x, y, z));
    }

    @Override
    public double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // No benefit, actually a drawback
      // if (!isWithinSection(x, y, z)) {
      //  return Double.POSITIVE_INFINITY;
      // }
      return Math.min(
          child1.nearestPathDistance(distanceStyle, x, y, z),
          child2.nearestPathDistance(distanceStyle, x, y, z));
    }

    @Override
    public double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // No benefit, actually a drawback
      // if (!isWithinSection(x, y, z)) {
      //   return Double.POSITIVE_INFINITY;
      // }
      return Math.min(
          child1.pathCenterDistance(distanceStyle, x, y, z),
          child2.pathCenterDistance(distanceStyle, x, y, z));
    }

    @Override
    public double outsideDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // No early-out for this one; all sections must be considered
      return Math.min(
          child1.outsideDistance(distanceStyle, x, y, z),
          child2.outsideDistance(distanceStyle, x, y, z));
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      // If this node bounding box does not overlap the plane's bounding box, exit early
      if (planeBounds != null && !planeBounds.overlaps(this.bounds)) {
        return false;
      }
      return child1.intersects(p, planeBounds, notablePoints, bounds)
          || child2.intersects(p, planeBounds, notablePoints, bounds);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return child1.intersects(geoShape) || child2.intersects(geoShape);
    }

    @Override
    public void getBounds(final Bounds bounds) {
      if (bounds instanceof XYZBounds) {
        this.bounds.addBounds((XYZBounds) bounds);
      } else {
        child1.getBounds(bounds);
        child2.getBounds(bounds);
      }
    }

    @Override
    public String toString() {
      return "PathNode (" + child1 + ") (" + child2 + ")";
    }
  }

  /**
   * Internal interface describing segment endpoint implementations. There are several different
   * such implementations, each corresponding to a different geometric conformation. Note well: This
   * is not necessarily a circle. There are four cases: (1) The path consists of a single endpoint.
   * In this case, we build a simple circle with the proper cutoff offset. (2) This is the end of a
   * path. The circle plane must be constructed to go through two supplied points and be
   * perpendicular to a connecting plane. (2.5) Intersection, but the path on both sides is linear.
   * We generate a circle, but we use the cutoff planes to limit its influence in the straight line
   * case. (3) This is an intersection in a path. We are supplied FOUR planes. If there are
   * intersections within bounds for both upper and lower, then we generate no circle at all. If
   * there is one intersection only, then we generate a plane that includes that intersection, as
   * well as the remaining cutoff plane/edge plane points.
   */
  private interface SegmentEndpoint extends PathComponent {}

  /** Base implementation of SegmentEndpoint */
  private static class BaseSegmentEndpoint extends GeoBaseBounds implements SegmentEndpoint {
    /** The previous path element */
    protected final PathComponent previous;

    /** The center point of the endpoint */
    protected final GeoPoint point;

    /** Null membership */
    protected static final Membership[] NO_MEMBERSHIP = new Membership[0];

    public BaseSegmentEndpoint(
        final PlanetModel planetModel, final PathComponent previous, final GeoPoint point) {
      super(planetModel);
      this.previous = previous;
      this.point = point;
    }

    @Override
    public boolean isWithin(final Vector point) {
      return false;
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return false;
    }

    @Override
    public boolean isWithinSection(final double x, final double y, final double z) {
      return true;
    }

    @Override
    public boolean isWithinSection(final Vector point) {
      return true;
    }

    @Override
    public double getStartingDistance(DistanceStyle distanceStyle) {
      if (previous == null) {
        return distanceStyle.toAggregationForm(0.0);
      }
      return distanceStyle.aggregateDistances(
          previous.getStartingDistance(distanceStyle), previous.fullPathDistance(distanceStyle));
    }

    @Override
    public double distance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      final double startingDistance = getStartingDistance(distanceStyle);
      final double pathDistance = pathDistance(distanceStyle, x, y, z);
      return distanceStyle.aggregateDistances(startingDistance, pathDistance);
    }

    @Override
    public DistancePair nearestDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return null;
      }
      final DistancePair rval =
          new DistancePair(
              pathCenterDistance(distanceStyle, x, y, z),
              distanceStyle.aggregateDistances(
                  getStartingDistance(distanceStyle), nearestPathDistance(distanceStyle, x, y, z)));
      return rval;
    }

    @Override
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      return distanceStyle.toAggregationForm(0.0);
    }

    @Override
    public double pathDeltaDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      final double theDistance =
          distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
      return distanceStyle.aggregateDistances(theDistance, theDistance);
    }

    @Override
    public double pathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
    }

    @Override
    public double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return distanceStyle.toAggregationForm(0.0);
    }

    @Override
    public double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
    }

    @Override
    public double outsideDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      return false;
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return false;
    }

    @Override
    public void getBounds(final Bounds bounds) {
      super.getBounds(bounds);
      bounds.addPoint(point);
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof BaseSegmentEndpoint)) {
        return false;
      }
      final BaseSegmentEndpoint other = (BaseSegmentEndpoint) o;
      return point.equals(other.point) && planetModel.equals(other.planetModel);
    }

    @Override
    public int hashCode() {
      return point.hashCode() + planetModel.hashCode();
    }

    @Override
    public String toString() {
      return "SegmentEndpoint (" + point + ")";
    }
  }

  /** Endpoint that's a simple circle. */
  private static class CircleSegmentEndpoint extends BaseSegmentEndpoint {
    /** A plane describing the circle */
    protected final SidedPlane circlePlane;

    /** No notable points from the circle itself */
    protected static final GeoPoint[] circlePoints = new GeoPoint[0];

    public CircleSegmentEndpoint(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint point,
        final GeoPoint upperPoint,
        final GeoPoint lowerPoint) {
      super(planetModel, previous, point);
      circlePlane = SidedPlane.constructSidedPlaneFromTwoPoints(point, upperPoint, lowerPoint);
      assert circlePlane.isWithin(point);
    }

    /**
     * Constructor for case (1). Generate a simple circle cutoff plane.
     *
     * @param planetModel is the planet model.
     * @param point is the center point.
     * @param upperPoint is a point that must be on the circle plane.
     * @param lowerPoint is another point that must be on the circle plane.
     */
    public CircleSegmentEndpoint(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint point,
        final Plane normalPlane,
        final GeoPoint upperPoint,
        final GeoPoint lowerPoint) {
      super(planetModel, previous, point);
      // Construct a sided plane that goes through the two points and whose normal is in the
      // normalPlane.
      this.circlePlane =
          SidedPlane.constructNormalizedPerpendicularSidedPlane(
              point, normalPlane, upperPoint, lowerPoint);
      assert circlePlane.isWithin(point);
    }

    /**
     * Constructor for case (3). Called by superclass only.
     *
     * @param point is the center point.
     * @param circlePlane is the circle plane.
     */
    protected CircleSegmentEndpoint(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint point,
        final SidedPlane circlePlane) {
      super(planetModel, previous, point);
      this.circlePlane = circlePlane;
    }

    @Override
    public boolean isWithin(final Vector point) {
      return circlePlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return circlePlane.isWithin(x, y, z);
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      return circlePlane.intersects(planetModel, p, notablePoints, circlePoints, bounds);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane, circlePoints, NO_MEMBERSHIP);
    }

    @Override
    public void getBounds(final Bounds bounds) {
      super.getBounds(bounds);
      bounds.addPlane(planetModel, circlePlane);
    }

    @Override
    public String toString() {
      return "CircleSegmentEndpoint: " + super.toString();
    }
  }

  /** Endpoint that's a single circle with cutoff(s). */
  private static class CutoffSingleCircleSegmentEndpoint extends CircleSegmentEndpoint {

    /** Pertinent cutoff plane from adjoining segments */
    protected final Membership[] cutoffPlanes;

    /** Notable points for this segment endpoint */
    private final GeoPoint[] notablePoints;

    private final SidedPlane cutoffPlane;

    /**
     * Constructor for case (2). Generate an endpoint, given a single cutoff plane plus upper and
     * lower edge points.
     *
     * @param planetModel is the planet model.
     * @param point is the center point.
     * @param cutoffPlane is the plane from the adjoining path segment marking the boundary between
     *     this endpoint and that segment.
     * @param topEdgePoint is a point on the cutoffPlane that should be also on the circle plane.
     * @param bottomEdgePoint is another point on the cutoffPlane that should be also on the circle
     *     plane.
     */
    public CutoffSingleCircleSegmentEndpoint(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint point,
        final SidedPlane cutoffPlane,
        final GeoPoint topEdgePoint,
        final GeoPoint bottomEdgePoint) {
      super(planetModel, previous, point, topEdgePoint, bottomEdgePoint);
      // Flip sign of cutoff plane
      this.cutoffPlane = new SidedPlane(cutoffPlane);
      this.cutoffPlanes = new Membership[] {this.cutoffPlane};
      this.notablePoints = new GeoPoint[] {topEdgePoint, bottomEdgePoint};
    }

    @Override
    public boolean isWithin(final Vector point) {
      return cutoffPlane.isWithin(point) && super.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return cutoffPlane.isWithin(x, y, z) && super.isWithin(x, y, z);
    }

    @Override
    public boolean isWithinSection(final Vector point) {
      return cutoffPlane.isWithin(point);
    }

    @Override
    public boolean isWithinSection(final double x, final double y, final double z) {
      return cutoffPlane.isWithin(x, y, z);
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      return circlePlane.intersects(
          planetModel, p, notablePoints, this.notablePoints, bounds, this.cutoffPlanes);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane, this.notablePoints, this.cutoffPlanes);
    }

    @Override
    public double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return super.nearestPathDistance(distanceStyle, x, y, z);
    }

    @Override
    public double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return super.pathCenterDistance(distanceStyle, x, y, z);
    }

    @Override
    public void getBounds(final Bounds bounds) {
      super.getBounds(bounds);
      bounds.addPlane(planetModel, circlePlane, cutoffPlane);
      bounds.addPlane(planetModel, cutoffPlane, circlePlane);
      bounds.addIntersection(planetModel, circlePlane, cutoffPlane);
      bounds.addIntersection(planetModel, cutoffPlane, circlePlane);
    }

    @Override
    public String toString() {
      return "CutoffSingleCircleSegmentEndpoint: " + super.toString();
    }
  }

  /**
   * Endpoint that's a dual circle with cutoff(s). This SegmentEndpoint is used when we have two
   * adjoining segments. (1) We construct two circles. Each circle uses the two segment endpoints
   * for one of the two segments, plus the one segment endpoint that is on the other side of the
   * segment's cutoff plane. (2) isWithin() is computed using both circles, using just the portion
   * that is within both segments' cutoff planes. If either matches, the point is included. (3)
   * intersects() is computed using both circles, with similar cutoffs. (4) bounds() uses both
   * circles too.
   */
  private static class CutoffDualCircleSegmentEndpoint extends BaseSegmentEndpoint {

    /** First circle */
    protected final SidedPlane circlePlane1;

    /** Second circle */
    protected final SidedPlane circlePlane2;

    /** Notable points for first circle */
    protected final GeoPoint[] notablePoints1;

    /** Notable points for second circle */
    protected final GeoPoint[] notablePoints2;

    /** Both cutoff planes are included here */
    protected final Membership[] cutoffPlanes;

    protected final SidedPlane boundaryPlane1;
    protected final SidedPlane boundaryPlane2;

    public CutoffDualCircleSegmentEndpoint(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint point,
        final SidedPlane prevCutoffPlane,
        final SidedPlane nextCutoffPlane,
        final GeoPoint prevURHC,
        final GeoPoint prevLRHC,
        final GeoPoint currentULHC,
        final GeoPoint currentLLHC) {
      // Initialize superclass
      super(planetModel, previous, point);
      // First plane consists of prev endpoints plus one of the current endpoints (the one past the
      // end of the prev segment)
      if (!prevCutoffPlane.isWithin(currentULHC)) {
        circlePlane1 =
            SidedPlane.constructNormalizedThreePointSidedPlane(
                point, prevURHC, prevLRHC, currentULHC);
        notablePoints1 = new GeoPoint[] {prevURHC, prevLRHC, currentULHC};
      } else if (!prevCutoffPlane.isWithin(currentLLHC)) {
        circlePlane1 =
            SidedPlane.constructNormalizedThreePointSidedPlane(
                point, prevURHC, prevLRHC, currentLLHC);
        notablePoints1 = new GeoPoint[] {prevURHC, prevLRHC, currentLLHC};
      } else {
        circlePlane1 = SidedPlane.constructSidedPlaneFromTwoPoints(point, prevURHC, prevLRHC);
        notablePoints1 = new GeoPoint[] {prevURHC, prevLRHC};
      }
      // Second plane consists of current endpoints plus one of the prev endpoints (the one past the
      // end of the current segment)
      if (!nextCutoffPlane.isWithin(prevURHC)) {
        circlePlane2 =
            SidedPlane.constructNormalizedThreePointSidedPlane(
                point, currentULHC, currentLLHC, prevURHC);
        notablePoints2 = new GeoPoint[] {currentULHC, currentLLHC, prevURHC};
      } else if (!nextCutoffPlane.isWithin(prevLRHC)) {
        circlePlane2 =
            SidedPlane.constructNormalizedThreePointSidedPlane(
                point, currentULHC, currentLLHC, prevLRHC);
        notablePoints2 = new GeoPoint[] {currentULHC, currentLLHC, prevLRHC};
      } else {
        circlePlane2 = SidedPlane.constructSidedPlaneFromTwoPoints(point, currentULHC, currentLLHC);
        notablePoints2 = new GeoPoint[] {currentULHC, currentLLHC};
      }
      this.boundaryPlane1 = new SidedPlane(prevCutoffPlane);
      this.boundaryPlane2 = new SidedPlane(nextCutoffPlane);
      this.cutoffPlanes = new Membership[] {boundaryPlane1, boundaryPlane2};
    }

    @Override
    public boolean isWithin(final Vector point) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(point)) {
          return false;
        }
      }
      return circlePlane1.isWithin(point) || circlePlane2.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x, y, z)) {
          return false;
        }
      }
      return circlePlane1.isWithin(x, y, z) || circlePlane2.isWithin(x, y, z);
    }

    @Override
    public boolean isWithinSection(final Vector point) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(point)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean isWithinSection(final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x, y, z)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      return circlePlane1.intersects(
              planetModel, p, notablePoints, this.notablePoints1, bounds, this.cutoffPlanes)
          || circlePlane2.intersects(
              planetModel, p, notablePoints, this.notablePoints2, bounds, this.cutoffPlanes);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane1, this.notablePoints1, this.cutoffPlanes)
          || geoShape.intersects(circlePlane2, this.notablePoints2, this.cutoffPlanes);
    }

    @Override
    public double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return super.nearestPathDistance(distanceStyle, x, y, z);
    }

    @Override
    public double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      return super.pathCenterDistance(distanceStyle, x, y, z);
    }

    @Override
    public void getBounds(final Bounds bounds) {
      super.getBounds(bounds);
      // System.out.println("Computing bounds for circlePlane1="+circlePlane1);
      bounds.addPlane(planetModel, circlePlane1, boundaryPlane1, boundaryPlane2);
      // System.out.println("Computing bounds for circlePlane2="+circlePlane2);
      bounds.addPlane(planetModel, circlePlane2, boundaryPlane1, boundaryPlane2);
      bounds.addPlane(planetModel, boundaryPlane1, circlePlane1, boundaryPlane2);
      bounds.addPlane(planetModel, boundaryPlane1, circlePlane2, boundaryPlane2);
      bounds.addPlane(planetModel, boundaryPlane2, circlePlane1, boundaryPlane1);
      bounds.addPlane(planetModel, boundaryPlane2, circlePlane2, boundaryPlane1);
      bounds.addIntersection(planetModel, circlePlane1, boundaryPlane1, boundaryPlane2);
      bounds.addIntersection(planetModel, circlePlane1, boundaryPlane2, boundaryPlane1);
      bounds.addIntersection(planetModel, circlePlane2, boundaryPlane1, boundaryPlane2);
      bounds.addIntersection(planetModel, circlePlane2, boundaryPlane2, boundaryPlane1);
    }

    @Override
    public String toString() {
      return "CutoffDualCircleSegmentEndpoint: " + super.toString();
    }
  }

  /** This is the pre-calculated data for a path segment. */
  private static class PathSegment extends GeoBaseBounds implements PathComponent {
    /** Previous path component */
    public final PathComponent previous;

    /** Starting point of the segment */
    public final GeoPoint start;

    /** End point of the segment */
    public final GeoPoint end;

    /** Place to keep any complete segment distances we've calculated so far */
    public final Map<DistanceStyle, Double> startDistanceCache = new ConcurrentHashMap<>(1);

    /** Normalized plane connecting the two points and going through world center */
    public final Plane normalizedConnectingPlane;

    /** Cutoff plane parallel to connecting plane representing one side of the path segment */
    public final SidedPlane upperConnectingPlane;

    /** Cutoff plane parallel to connecting plane representing the other side of the path segment */
    public final SidedPlane lowerConnectingPlane;

    /** Plane going through the center and start point, marking the start edge of the segment */
    public final SidedPlane startCutoffPlane;

    /** Plane going through the center and end point, marking the end edge of the segment */
    public final SidedPlane endCutoffPlane;

    /** Upper right hand corner of segment */
    public final GeoPoint URHC;

    /** Lower right hand corner of segment */
    public final GeoPoint LRHC;

    /** Upper left hand corner of segment */
    public final GeoPoint ULHC;

    /** Lower left hand corner of segment */
    public final GeoPoint LLHC;

    /** Notable points for the upper connecting plane */
    public final GeoPoint[] upperConnectingPlanePoints;

    /** Notable points for the lower connecting plane */
    public final GeoPoint[] lowerConnectingPlanePoints;

    /**
     * Construct a path segment.
     *
     * @param planetModel is the planet model.
     * @param start is the starting point.
     * @param end is the ending point.
     * @param normalizedConnectingPlane is the connecting plane.
     * @param planeBoundingOffset is the linear offset from the connecting plane to either side.
     */
    public PathSegment(
        final PlanetModel planetModel,
        final PathComponent previous,
        final GeoPoint start,
        final GeoPoint end,
        final Plane normalizedConnectingPlane,
        final double planeBoundingOffset) {
      super(planetModel);
      this.previous = previous;
      this.start = start;
      this.end = end;
      this.normalizedConnectingPlane = normalizedConnectingPlane;

      // Either start or end should be on the correct side
      upperConnectingPlane = new SidedPlane(start, normalizedConnectingPlane, -planeBoundingOffset);
      lowerConnectingPlane = new SidedPlane(start, normalizedConnectingPlane, planeBoundingOffset);
      assert upperConnectingPlane.isWithin(start);
      assert upperConnectingPlane.isWithin(end);
      assert lowerConnectingPlane.isWithin(start);
      assert lowerConnectingPlane.isWithin(end);
      // Cutoff planes use opposite endpoints as correct side examples
      startCutoffPlane = new SidedPlane(end, normalizedConnectingPlane, start);
      assert startCutoffPlane.isWithin(end);
      assert startCutoffPlane.isWithin(start);
      endCutoffPlane = new SidedPlane(start, normalizedConnectingPlane, end);
      assert endCutoffPlane.isWithin(start);
      assert endCutoffPlane.isWithin(end);
      final Membership[] upperSide = new Membership[] {upperConnectingPlane};
      final Membership[] lowerSide = new Membership[] {lowerConnectingPlane};
      final Membership[] startSide = new Membership[] {startCutoffPlane};
      final Membership[] endSide = new Membership[] {endCutoffPlane};
      GeoPoint[] points;
      points =
          upperConnectingPlane.findIntersections(planetModel, startCutoffPlane, lowerSide, endSide);
      if (points.length == 0) {
        throw new IllegalArgumentException(
            "Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.ULHC = points[0];
      points =
          upperConnectingPlane.findIntersections(planetModel, endCutoffPlane, lowerSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException(
            "Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.URHC = points[0];
      points =
          lowerConnectingPlane.findIntersections(planetModel, startCutoffPlane, upperSide, endSide);
      if (points.length == 0) {
        throw new IllegalArgumentException(
            "Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.LLHC = points[0];
      points =
          lowerConnectingPlane.findIntersections(planetModel, endCutoffPlane, upperSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException(
            "Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.LRHC = points[0];
      upperConnectingPlanePoints = new GeoPoint[] {ULHC, URHC};
      lowerConnectingPlanePoints = new GeoPoint[] {LLHC, LRHC};
    }

    @Override
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      return distanceStyle.toAggregationForm(
          distanceStyle.computeDistance(start, end.x, end.y, end.z));
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return startCutoffPlane.isWithin(x, y, z)
          && endCutoffPlane.isWithin(x, y, z)
          && upperConnectingPlane.isWithin(x, y, z)
          && lowerConnectingPlane.isWithin(x, y, z);
    }

    @Override
    public boolean isWithin(final Vector v) {
      return isWithin(v.x, v.y, v.z);
    }

    @Override
    public boolean isWithinSection(final Vector point) {
      return startCutoffPlane.isWithin(point) && endCutoffPlane.isWithin(point);
    }

    @Override
    public boolean isWithinSection(final double x, final double y, final double z) {
      return startCutoffPlane.isWithin(x, y, z) && endCutoffPlane.isWithin(x, y, z);
    }

    @Override
    public double getStartingDistance(final DistanceStyle distanceStyle) {
      Double dist = startDistanceCache.get(distanceStyle);
      if (dist == null) {
        dist = computeStartingDistance(distanceStyle);
        startDistanceCache.put(distanceStyle, dist);
      }
      return dist.doubleValue();
    }

    @Override
    public double distance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      final double startingDistance = getStartingDistance(distanceStyle);
      final double pathDistance = pathDistance(distanceStyle, x, y, z);
      // System.out.println("In PathSegment distance(), startingDistance = "+startingDistance+"
      // pathDistance = "+pathDistance);
      return distanceStyle.fromAggregationForm(
          distanceStyle.aggregateDistances(startingDistance, pathDistance));
    }

    @Override
    public DistancePair nearestDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithinSection(x, y, z)) {
        return null;
      }
      final DistancePair rval =
          new DistancePair(
              pathCenterDistance(distanceStyle, x, y, z),
              distanceStyle.aggregateDistances(
                  getStartingDistance(distanceStyle), nearestPathDistance(distanceStyle, x, y, z)));
      return rval;
    }

    private double computeStartingDistance(final DistanceStyle distanceStyle) {
      if (previous == null) {
        return 0.0;
      }
      return distanceStyle.aggregateDistances(
          previous.getStartingDistance(distanceStyle), previous.fullPathDistance(distanceStyle));
    }

    @Override
    public double pathCenterDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // Computes the distance to the path center from the specified point, or returns
      // POSITIVE_INFINITY if the point is outside the path section.

      // First, if this point is outside the endplanes of the segment, return POSITIVE_INFINITY.
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      // (1) Compute normalizedPerpPlane.  If degenerate, then there is no such plane, which means
      // that the point given is insufficient to distinguish between a family of such planes.
      // This can happen only if the point is one of the "poles", imagining the normalized plane
      // to be the "equator".  In that case, the distance returned should be zero.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION) {
        return distanceStyle.computeDistance(start, x, y, z);
      }
      final double normFactor = 1.0 / magnitude;
      final Plane normalizedPerpPlane =
          new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);

      final GeoPoint[] intersectionPoints =
          normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0) {
        throw new RuntimeException(
            "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
      } else if (intersectionPoints.length == 1) {
        thePoint = intersectionPoints[0];
      } else {
        if (startCutoffPlane.isWithin(intersectionPoints[0])
            && endCutoffPlane.isWithin(intersectionPoints[0])) {
          thePoint = intersectionPoints[0];
        } else if (startCutoffPlane.isWithin(intersectionPoints[1])
            && endCutoffPlane.isWithin(intersectionPoints[1])) {
          thePoint = intersectionPoints[1];
        } else {
          throw new RuntimeException(
              "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
        }
      }
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(thePoint, x, y, z));
    }

    @Override
    public double nearestPathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // Computes the distance along the path to a point on the path where a perpendicular plane
      // goes through the specified point.
      // First, if this point is outside the endplanes of the segment, return POSITIVE_INFINITY.
      if (!isWithinSection(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      // (1) Compute normalizedPerpPlane.  If degenerate, then there is no such plane, which means
      // that the point given is insufficient to distinguish between a family of such planes.
      // This can happen only if the point is one of the "poles", imagining the normalized plane
      // to be the "equator".  In that case, the distance returned should be zero.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION) {
        return distanceStyle.toAggregationForm(0.0);
      }
      final double normFactor = 1.0 / magnitude;
      final Plane normalizedPerpPlane =
          new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);

      final GeoPoint[] intersectionPoints =
          normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0) {
        throw new RuntimeException(
            "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
      } else if (intersectionPoints.length == 1) {
        thePoint = intersectionPoints[0];
      } else {
        if (startCutoffPlane.isWithin(intersectionPoints[0])
            && endCutoffPlane.isWithin(intersectionPoints[0])) {
          thePoint = intersectionPoints[0];
        } else if (startCutoffPlane.isWithin(intersectionPoints[1])
            && endCutoffPlane.isWithin(intersectionPoints[1])) {
          thePoint = intersectionPoints[1];
        } else {
          throw new RuntimeException(
              "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
        }
      }
      return distanceStyle.toAggregationForm(
          distanceStyle.computeDistance(start, thePoint.x, thePoint.y, thePoint.z));
    }

    @Override
    public double pathDeltaDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // Represents the cost of an "excursion" to and
      // from the path.

      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      // (1) Compute normalizedPerpPlane.  If degenerate, then return point distance from start to
      // point.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION) {
        final double theDistance = distanceStyle.computeDistance(start, x, y, z);
        return distanceStyle.aggregateDistances(theDistance, theDistance);
      }
      final double normFactor = 1.0 / magnitude;
      final Plane normalizedPerpPlane =
          new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);

      // Old computation: too expensive, because it calculates the intersection point twice.
      // return distanceStyle.computeDistance(planetModel, normalizedConnectingPlane, x, y, z,
      // startCutoffPlane, endCutoffPlane) +
      //  distanceStyle.computeDistance(planetModel, normalizedPerpPlane, start.x, start.y, start.z,
      // upperConnectingPlane, lowerConnectingPlane);

      final GeoPoint[] intersectionPoints =
          normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0) {
        throw new RuntimeException(
            "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
      } else if (intersectionPoints.length == 1) {
        thePoint = intersectionPoints[0];
      } else {
        if (startCutoffPlane.isWithin(intersectionPoints[0])
            && endCutoffPlane.isWithin(intersectionPoints[0])) {
          thePoint = intersectionPoints[0];
        } else if (startCutoffPlane.isWithin(intersectionPoints[1])
            && endCutoffPlane.isWithin(intersectionPoints[1])) {
          thePoint = intersectionPoints[1];
        } else {
          throw new RuntimeException(
              "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
        }
      }
      final double theDistance =
          distanceStyle.toAggregationForm(distanceStyle.computeDistance(thePoint, x, y, z));
      return distanceStyle.aggregateDistances(theDistance, theDistance);
    }

    @Override
    public double pathDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {

      // Computes the distance along the path to a perpendicular point to the desired point,
      // and adds the distance from the path to the desired point.

      if (!isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }

      // (1) Compute normalizedPerpPlane.  If degenerate, then return point distance from start to
      // point.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION) {
        return distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, x, y, z));
      }
      final double normFactor = 1.0 / magnitude;
      final Plane normalizedPerpPlane =
          new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);

      // Old computation: too expensive, because it calculates the intersection point twice.
      // return distanceStyle.computeDistance(planetModel, normalizedConnectingPlane, x, y, z,
      // startCutoffPlane, endCutoffPlane) +
      //  distanceStyle.computeDistance(planetModel, normalizedPerpPlane, start.x, start.y, start.z,
      // upperConnectingPlane, lowerConnectingPlane);

      final GeoPoint[] intersectionPoints =
          normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0) {
        throw new RuntimeException(
            "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
      } else if (intersectionPoints.length == 1) {
        thePoint = intersectionPoints[0];
      } else {
        if (startCutoffPlane.isWithin(intersectionPoints[0])
            && endCutoffPlane.isWithin(intersectionPoints[0])) {
          thePoint = intersectionPoints[0];
        } else if (startCutoffPlane.isWithin(intersectionPoints[1])
            && endCutoffPlane.isWithin(intersectionPoints[1])) {
          thePoint = intersectionPoints[1];
        } else {
          throw new RuntimeException(
              "Can't find world intersection for point x=" + x + " y=" + y + " z=" + z);
        }
      }
      return distanceStyle.aggregateDistances(
          distanceStyle.toAggregationForm(distanceStyle.computeDistance(thePoint, x, y, z)),
          distanceStyle.toAggregationForm(
              distanceStyle.computeDistance(start, thePoint.x, thePoint.y, thePoint.z)));
    }

    @Override
    public double outsideDistance(
        final DistanceStyle distanceStyle, final double x, final double y, final double z) {

      // Computes the minimum distance from the point to any of the segment's bounding planes.
      // Only computes distances outside of the shape; inside, a value of 0.0 is returned.
      final double upperDistance =
          distanceStyle.computeDistance(
              planetModel,
              upperConnectingPlane,
              x,
              y,
              z,
              lowerConnectingPlane,
              startCutoffPlane,
              endCutoffPlane);
      final double lowerDistance =
          distanceStyle.computeDistance(
              planetModel,
              lowerConnectingPlane,
              x,
              y,
              z,
              upperConnectingPlane,
              startCutoffPlane,
              endCutoffPlane);
      final double startDistance =
          distanceStyle.computeDistance(
              planetModel,
              startCutoffPlane,
              x,
              y,
              z,
              endCutoffPlane,
              lowerConnectingPlane,
              upperConnectingPlane);
      final double endDistance =
          distanceStyle.computeDistance(
              planetModel,
              endCutoffPlane,
              x,
              y,
              z,
              startCutoffPlane,
              lowerConnectingPlane,
              upperConnectingPlane);
      final double ULHCDistance = distanceStyle.computeDistance(ULHC, x, y, z);
      final double URHCDistance = distanceStyle.computeDistance(URHC, x, y, z);
      final double LLHCDistance = distanceStyle.computeDistance(LLHC, x, y, z);
      final double LRHCDistance = distanceStyle.computeDistance(LRHC, x, y, z);
      return distanceStyle.toAggregationForm(
          Math.min(
              Math.min(
                  Math.min(upperDistance, lowerDistance), Math.min(startDistance, endDistance)),
              Math.min(
                  Math.min(ULHCDistance, URHCDistance), Math.min(LLHCDistance, LRHCDistance))));
    }

    @Override
    public boolean intersects(
        final Plane p,
        final XYZBounds planeBounds,
        final GeoPoint[] notablePoints,
        final Membership[] bounds) {
      return upperConnectingPlane.intersects(
              planetModel,
              p,
              notablePoints,
              upperConnectingPlanePoints,
              bounds,
              lowerConnectingPlane,
              startCutoffPlane,
              endCutoffPlane)
          || lowerConnectingPlane.intersects(
              planetModel,
              p,
              notablePoints,
              lowerConnectingPlanePoints,
              bounds,
              upperConnectingPlane,
              startCutoffPlane,
              endCutoffPlane);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(
              upperConnectingPlane,
              upperConnectingPlanePoints,
              lowerConnectingPlane,
              startCutoffPlane,
              endCutoffPlane)
          || geoShape.intersects(
              lowerConnectingPlane,
              lowerConnectingPlanePoints,
              upperConnectingPlane,
              startCutoffPlane,
              endCutoffPlane);
    }

    @Override
    public void getBounds(final Bounds bounds) {
      super.getBounds(bounds);
      // We need to do all bounding planes as well as corner points
      bounds
          .addPoint(start)
          .addPoint(end)
          .addPoint(ULHC)
          .addPoint(URHC)
          .addPoint(LRHC)
          .addPoint(LLHC)
          .addPlane(
              planetModel,
              upperConnectingPlane,
              lowerConnectingPlane,
              startCutoffPlane,
              endCutoffPlane)
          .addPlane(
              planetModel,
              lowerConnectingPlane,
              upperConnectingPlane,
              startCutoffPlane,
              endCutoffPlane)
          .addPlane(
              planetModel,
              startCutoffPlane,
              endCutoffPlane,
              upperConnectingPlane,
              lowerConnectingPlane)
          .addPlane(
              planetModel,
              endCutoffPlane,
              startCutoffPlane,
              upperConnectingPlane,
              lowerConnectingPlane)
          .addIntersection(
              planetModel,
              upperConnectingPlane,
              startCutoffPlane,
              lowerConnectingPlane,
              endCutoffPlane)
          .addIntersection(
              planetModel,
              startCutoffPlane,
              lowerConnectingPlane,
              endCutoffPlane,
              upperConnectingPlane)
          .addIntersection(
              planetModel,
              lowerConnectingPlane,
              endCutoffPlane,
              upperConnectingPlane,
              startCutoffPlane)
          .addIntersection(
              planetModel,
              endCutoffPlane,
              upperConnectingPlane,
              startCutoffPlane,
              lowerConnectingPlane);
    }

    @Override
    public String toString() {
      return "PathSegment (" + ULHC + ", " + URHC + ", " + LRHC + ", " + LLHC + ")";
    }
  }

  private static class TreeBuilder {
    private final List<PathComponent> componentStack;
    private final IntArrayList depthStack;

    public TreeBuilder(final int max) {
      componentStack = new ArrayList<>(max);
      depthStack = new IntArrayList(max);
    }

    public void addComponent(final PathComponent component) {
      componentStack.add(component);
      depthStack.add(0);
      while (depthStack.size() >= 2) {
        if (depthStack.get(depthStack.size() - 1) == depthStack.get(depthStack.size() - 2)) {
          mergeTop();
        } else {
          break;
        }
      }
    }

    public PathComponent getRoot() {
      if (componentStack.size() == 0) {
        return null;
      }
      while (componentStack.size() > 1) {
        mergeTop();
      }
      return componentStack.get(0);
    }

    private void mergeTop() {
      depthStack.removeAt(depthStack.size() - 1);
      PathComponent secondComponent = componentStack.remove(componentStack.size() - 1);
      int newDepth = depthStack.removeAt(depthStack.size() - 1) + 1;
      PathComponent firstComponent = componentStack.remove(componentStack.size() - 1);
      depthStack.add(newDepth);
      componentStack.add(new PathNode(firstComponent, secondComponent));
    }
  }
}
