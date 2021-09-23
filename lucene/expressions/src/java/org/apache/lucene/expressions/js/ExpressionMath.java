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

package org.apache.lucene.expressions.js;

import static org.apache.lucene.util.SloppyMath.haversinMeters;
import static org.apache.lucene.util.SloppyMath.haversinSortKey;

/** Helper class holding static methods for js math functions */
public final class ExpressionMath {

  private ExpressionMath() {}

  private static final double TO_KILOMETERS = 1D / 1000;

  /**
   * Returns the Haversine distance in kilometers between two points specified in decimal degrees
   * (latitude/longitude). This works correctly even if the dateline is between the two points.
   *
   * <p>Error is at most 4E-1 (40cm) from the actual haversine distance, but is typically much
   * smaller for reasonable distances: around 1E-5 (0.01mm) for distances less than 1000km.
   *
   * @param lat1 Latitude of the first point.
   * @param lon1 Longitude of the first point.
   * @param lat2 Latitude of the second point.
   * @param lon2 Longitude of the second point.
   * @return distance in kilometers.
   */
  public static double haversinKilometers(double lat1, double lon1, double lat2, double lon2) {
    return haversinMeters(haversinSortKey(lat1, lon1, lat2, lon2)) * TO_KILOMETERS;
  }
}
