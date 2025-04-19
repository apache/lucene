/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package org.apache.lucene.geo;
//
// import org.apache.lucene.tests.util.LuceneTestCase;
//
//// TODO: To be removed before commit
// public class TestSimpleRectangleBenchmark extends LuceneTestCase {
//    // Test cases
//    private static final double[] LATITUDES = {0.0, 45.0, 89.0, -45.0, -89.0};
//    private static final double[] LONGITUDES = {0.0, 179.0, -179.0, 90.0, -90.0};
//    private static final double[] RADIUSES = {100, 1000, 10000, 100000, 1000000};
//
//    private static void runSingleTest(String testName, double lat, double lon, double radius) {
//        // No Warmup
//        for (int i = 0; i < 1000; i++) {
//            Rectangle.fromPointDistance(lat, lon, radius);
//        }
//
//        // Single measurement
//        long start = System.nanoTime();
//        Rectangle.fromPointDistance(lat, lon, radius);
//        long duration = System.nanoTime() - start;
//
//        System.out.printf("%-30s time: %.3f µs%n",
//                testName,
//                duration / 1000.0);
//    }
//
//    public void testBenchmark() {
//        System.out.println("Running Rectangle.fromPointDistance benchmarks...");
//        System.out.println("----------------------------------------");
//
//        // Test various combinations
//        for (double lat : LATITUDES) {
//            for (double lon : LONGITUDES) {
//                for (double radius : RADIUSES) {
//                    String testName = String.format("lat=%.1f,lon=%.1f,r=%.1f", lat, lon, radius);
//                    runSingleTest(testName, lat, lon, radius);
//                }
//            }
//        }
//
//        // Special cases
//        System.out.println("\nSpecial cases:");
//        System.out.println("----------------------------------------");
//
//        runSingleTest("Near dateline", 0.0, 179.9, 10000);
//        runSingleTest("Near pole", 89.9, 0.0, 10000);
//        runSingleTest("Equator", 0.0, 0.0, 10000);
//    }
// }
