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

/**
 * Factory for {@link GeoBBox}.
 *
 * @lucene.experimental
 */
public class GeoBBoxFactory {
  private GeoBBoxFactory() {}

  /**
   * Create a geobbox of the right kind given the specified bounds.
   *
   * @param planetModel is the planet model
   * @param topLat is the top latitude
   * @param bottomLat is the bottom latitude
   * @param leftLon is the left longitude
   * @param rightLon is the right longitude
   * @return a GeoBBox corresponding to what was specified.
   */
  public static GeoBBox makeGeoBBox(
      final PlanetModel planetModel,
      double topLat,
      double bottomLat,
      double leftLon,
      double rightLon) {
    // System.err.println("Making rectangle for topLat="+topLat*180.0/Math.PI+",
    // bottomLat="+bottomLat*180.0/Math.PI+", leftLon="+leftLon*180.0/Math.PI+",
    // rightlon="+rightLon*180.0/Math.PI);
    if (topLat > Math.PI * 0.5) {
      topLat = Math.PI * 0.5;
    }
    if (bottomLat < -Math.PI * 0.5) {
      bottomLat = -Math.PI * 0.5;
    }
    if (leftLon < -Math.PI) {
      leftLon = -Math.PI;
    }
    if (rightLon > Math.PI) {
      rightLon = Math.PI;
    }
    if ((longitudesEquals(leftLon, -Math.PI) && longitudesEquals(rightLon, Math.PI))
        || (longitudesEquals(rightLon, -Math.PI) && longitudesEquals(leftLon, Math.PI))) {
      if (isNorthPole(topLat) && isSouthPole(bottomLat)) {
        return new GeoWorld(planetModel);
      }
      if (latitudesEquals(topLat, bottomLat)) {
        if (isNorthPole(topLat)) {
          return new GeoDegeneratePoint(planetModel, topLat, 0.0);
        } else if (isSouthPole(bottomLat)) {
          return new GeoDegeneratePoint(planetModel, bottomLat, 0.0);
        }
        return new GeoDegenerateLatitudeZone(planetModel, topLat);
      }
      if (isNorthPole(topLat)) {
        return new GeoNorthLatitudeZone(planetModel, bottomLat);
      } else if (isSouthPole(bottomLat)) {
        return new GeoSouthLatitudeZone(planetModel, topLat);
      }
      return new GeoLatitudeZone(planetModel, topLat, bottomLat);
    }
    // System.err.println(" not latitude zone");
    double extent = rightLon - leftLon;
    if (extent < 0.0) {
      extent += Math.PI * 2.0;
    }
    if (isNorthPole(topLat) && isSouthPole(bottomLat)) {
      if (longitudesEquals(leftLon, rightLon)) {
        return new GeoDegenerateLongitudeSlice(planetModel, leftLon);
      }
      if (extent >= Math.PI) {
        return new GeoWideLongitudeSlice(planetModel, leftLon, rightLon);
      }

      return new GeoLongitudeSlice(planetModel, leftLon, rightLon);
    }
    // System.err.println(" not longitude slice");
    if (longitudesEquals(leftLon, rightLon)) {
      if (latitudesEquals(topLat, bottomLat)) {
        return new GeoDegeneratePoint(planetModel, topLat, leftLon);
      }
      return new GeoDegenerateVerticalLine(planetModel, topLat, bottomLat, leftLon);
    }
    // System.err.println(" not vertical line");
    if (extent >= GeoWideRectangle.MIN_WIDE_EXTENT) {
      if (latitudesEquals(topLat, bottomLat)) {
        if (isNorthPole(topLat)) {
          return new GeoDegeneratePoint(planetModel, topLat, 0.0);
        } else if (isSouthPole(bottomLat)) {
          return new GeoDegeneratePoint(planetModel, bottomLat, 0.0);
        }
        // System.err.println(" wide degenerate line");
        return new GeoWideDegenerateHorizontalLine(planetModel, topLat, leftLon, rightLon);
      }
      if (isNorthPole(topLat)) {
        return new GeoWideNorthRectangle(planetModel, bottomLat, leftLon, rightLon);
      } else if (isSouthPole(bottomLat)) {
        return new GeoWideSouthRectangle(planetModel, topLat, leftLon, rightLon);
      }
      // System.err.println(" wide rect");
      return new GeoWideRectangle(planetModel, topLat, bottomLat, leftLon, rightLon);
    }
    if (latitudesEquals(topLat, bottomLat)) {
      if (isNorthPole(topLat)) {
        return new GeoDegeneratePoint(planetModel, topLat, 0.0);
      } else if (isSouthPole(bottomLat)) {
        return new GeoDegeneratePoint(planetModel, bottomLat, 0.0);
      }
      // System.err.println(" horizontal line");
      return new GeoDegenerateHorizontalLine(planetModel, topLat, leftLon, rightLon);
    }
    if (isNorthPole(topLat)) {
      return new GeoNorthRectangle(planetModel, bottomLat, leftLon, rightLon);
    } else if (isSouthPole(bottomLat)) {
      return new GeoSouthRectangle(planetModel, topLat, leftLon, rightLon);
    }
    // System.err.println(" rectangle");
    return new GeoRectangle(planetModel, topLat, bottomLat, leftLon, rightLon);
  }

  private static boolean isNorthPole(double lat) {
    return latitudesEquals(lat, Math.PI * 0.5);
  }

  private static boolean isSouthPole(double lat) {
    return latitudesEquals(lat, -Math.PI * 0.5);
  }

  private static boolean latitudesEquals(double lat1, double lat2) {
    // it is not enough with using the MINIMUM_ANGULAR_RESOLUTION, check as well the sin values
    // just in case they describe the same plane
    return Math.abs(lat1 - lat2) < Vector.MINIMUM_ANGULAR_RESOLUTION
        || Math.sin(lat1) == Math.sin(lat2);
  }

  private static boolean longitudesEquals(double lon1, double lon2) {
    return Math.abs(lon1 - lon2) < Vector.MINIMUM_ANGULAR_RESOLUTION;
  }

  /**
   * Create a geobbox of the right kind given the specified {@link LatLonBounds}.
   *
   * @param planetModel is the planet model
   * @param bounds are the bounds
   * @return a GeoBBox corresponding to what was specified.
   */
  public static GeoBBox makeGeoBBox(final PlanetModel planetModel, LatLonBounds bounds) {
    final double topLat =
        (bounds.checkNoTopLatitudeBound()) ? Math.PI * 0.5 : bounds.getMaxLatitude();
    final double bottomLat =
        (bounds.checkNoBottomLatitudeBound()) ? -Math.PI * 0.5 : bounds.getMinLatitude();
    final double leftLon = (bounds.checkNoLongitudeBound()) ? -Math.PI : bounds.getLeftLongitude();
    final double rightLon = (bounds.checkNoLongitudeBound()) ? Math.PI : bounds.getRightLongitude();
    return makeGeoBBox(planetModel, topLat, bottomLat, leftLon, rightLon);
  }
}
