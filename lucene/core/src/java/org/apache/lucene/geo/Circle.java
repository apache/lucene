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

/**
 * Represents a circle on the Earth's surface defined by a center point and radius.
 *
 * <p>The circle is defined using:
 *
 * <ul>
 *   <li>Center point (latitude/longitude in degrees)
 *   <li>Radius in meters
 * </ul>
 *
 * <p>Important Notes:
 *
 * <ul>
 *   <li>The circle is approximated on the spherical Earth model
 *   <li>For very large circles or circles near poles, consider using polygons instead
 *   <li>Dateline crossing is handled automatically
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a circle with 1km radius around the Eiffel Tower
 * Circle circle = new Circle(48.8584, 2.2945, 1000);
 *
 * // Create a query using this circle
 * Query query = LatLonShape.newDistanceQuery("location", circle);
 * }</pre>
 *
 * @lucene.experimental
 */
public final class Circle extends LatLonGeometry {
  /** Center latitude of the circle in degrees (-90 to 90) */
  private final double lat;

  /** Center longitude of the circle in degrees (-180 to 180) */
  private final double lon;

  /** radius of the circle in meters */
  private final double radiusMeters;

  /** Creates a new circle from the supplied latitude/longitude center and a radius in meters.. */
  public Circle(double lat, double lon, double radiusMeters) {
    GeoUtils.checkLatitude(lat);
    GeoUtils.checkLongitude(lon);
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("radiusMeters: '" + radiusMeters + "' is invalid");
    }
    this.lat = lat;
    this.lon = lon;
    this.radiusMeters = radiusMeters;
  }

  /** Returns the center's latitude */
  public double getLat() {
    return lat;
  }

  /** Returns the center's longitude */
  public double getLon() {
    return lon;
  }

  /** Returns the radius in meters */
  public double getRadius() {
    return radiusMeters;
  }

  @Override
  protected Component2D toComponent2D() {
    return Circle2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Circle)) return false;
    Circle circle = (Circle) o;
    return Double.compare(lat, circle.lat) == 0
        && Double.compare(lon, circle.lon) == 0
        && Double.compare(radiusMeters, circle.radiusMeters) == 0;
  }

  @Override
  public int hashCode() {
    int result = Double.hashCode(lat);
    result = 31 * result + Double.hashCode(lon);
    result = 31 * result + Double.hashCode(radiusMeters);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Circle(");
    sb.append("[" + lat + "," + lon + "]");
    sb.append(" radius = " + radiusMeters + " meters");
    sb.append(')');
    return sb.toString();
  }
}
