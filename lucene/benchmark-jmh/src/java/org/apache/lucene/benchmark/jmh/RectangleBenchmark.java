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
package org.apache.lucene.benchmark.jmh;

import static java.lang.Math.PI;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.lucene.geo.GeoUtils.EARTH_MEAN_RADIUS_METERS;
import static org.apache.lucene.geo.GeoUtils.MAX_LAT_RADIANS;
import static org.apache.lucene.geo.GeoUtils.MAX_LON_RADIANS;
import static org.apache.lucene.geo.GeoUtils.MIN_LAT_RADIANS;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_RADIANS;
import static org.apache.lucene.geo.GeoUtils.checkLatitude;
import static org.apache.lucene.geo.GeoUtils.checkLongitude;
import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.cos;

import java.util.concurrent.TimeUnit;
import org.apache.lucene.geo.Rectangle;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 2)
public class RectangleBenchmark {

  @State(Scope.Benchmark)
  public static class ExecutionPlan {
    // Test cases representing different geographical scenarios
    @Param({
      "0.0", // Equator
      "45.0", // Mid-latitude
      "89.0", // Near pole
      "-45.0", // Southern hemisphere
      "-89.0" // Near south pole
    })
    private double latitude;

    @Param({
      "0.0", // Prime meridian
      "179.0", // Near dateline
      "-179.0", // Near dateline other side
      "90.0", // Mid-longitude
      "-90.0" // Mid-longitude west
    })
    private double longitude;

    @Param({
      "100", // Small radius (100m)
      "1000", // 1km
      "10000", // 10km
      "100000", // 100km
      "1000000" // 1000km
    })
    private double radiusMeters;
  }

  @Benchmark
  public Rectangle benchmarkFromPointDistanceSloppySin(ExecutionPlan plan) {
    return Rectangle.fromPointDistance(plan.latitude, plan.longitude, plan.radiusMeters);
  }

  @Benchmark
  public Rectangle benchmarkFromPointDistanceStandardSin(ExecutionPlan plan) {
    return fromPointDistanceStandardSin(plan.latitude, plan.longitude, plan.radiusMeters);
  }

  private static Rectangle fromPointDistanceStandardSin(
      final double centerLat, final double centerLon, final double radiusMeters) {
    checkLatitude(centerLat);
    checkLongitude(centerLon);
    final double radLat = Math.toRadians(centerLat);
    final double radLon = Math.toRadians(centerLon);
    // LUCENE-7143
    double radDistance = (radiusMeters + 7E-2) / EARTH_MEAN_RADIUS_METERS;
    double minLat = radLat - radDistance;
    double maxLat = radLat + radDistance;
    double minLon;
    double maxLon;

    if (minLat > MIN_LAT_RADIANS && maxLat < MAX_LAT_RADIANS) {
      double deltaLon = asin(Math.sin(radDistance) / cos(radLat));
      minLon = radLon - deltaLon;
      if (minLon < MIN_LON_RADIANS) {
        minLon += 2d * PI;
      }
      maxLon = radLon + deltaLon;
      if (maxLon > MAX_LON_RADIANS) {
        maxLon -= 2d * PI;
      }
    } else {
      // a pole is within the distance
      minLat = max(minLat, MIN_LAT_RADIANS);
      maxLat = min(maxLat, MAX_LAT_RADIANS);
      minLon = MIN_LON_RADIANS;
      maxLon = MAX_LON_RADIANS;
    }

    return new Rectangle(
        Math.toDegrees(minLat),
        Math.toDegrees(maxLat),
        Math.toDegrees(minLon),
        Math.toDegrees(maxLon));
  }
}
