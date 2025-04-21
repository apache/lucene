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

import org.apache.lucene.geo.Rectangle;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class RectangleBenchmark {

    // Test cases representing different geographical scenarios
    @Param({
            "0.0",    // Equator
            "45.0",   // Mid-latitude
            "89.0",   // Near pole
            "-45.0",  // Southern hemisphere
            "-89.0"   // Near south pole
    })
    private double latitude;

    @Param({
            "0.0",     // Prime meridian
            "179.0",   // Near dateline
            "-179.0",  // Near dateline other side
            "90.0",    // Mid-longitude
            "-90.0"    // Mid-longitude west
    })
    private double longitude;

    @Param({
            "100",      // Small radius (100m)
            "1000",     // 1km
            "10000",    // 10km
            "100000",   // 100km
            "1000000"   // 1000km
    })
    private double radiusMeters;

    @Benchmark
    public Rectangle benchmarkFromPointDistance() {
        return Rectangle.fromPointDistance(latitude, longitude, radiusMeters);
    }

    @Benchmark
    public Rectangle benchmarkFromPointDistanceNearDateline() {
        // Test specific case near the dateline
        return Rectangle.fromPointDistance(0.0, 179.9, 10000);
    }

    @Benchmark
    public Rectangle benchmarkFromPointDistanceNearPole() {
        // Test specific case near the pole
        return Rectangle.fromPointDistance(89.9, 0.0, 10000);
    }

    @Benchmark
    public Rectangle benchmarkFromPointDistanceEquator() {
        // Test specific case at equator
        return Rectangle.fromPointDistance(0.0, 0.0, 10000);
    }

    // Method to run the benchmark from IDE
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RectangleBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}