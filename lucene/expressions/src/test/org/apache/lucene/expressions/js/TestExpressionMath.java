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

import static org.apache.lucene.expressions.js.ExpressionMath.haversinKilometers;

import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestExpressionMath extends LuceneTestCase {

  public void testHaversin() {
    assertTrue(Double.isNaN(haversinKilometers(1, 1, 1, Double.NaN)));
    assertTrue(Double.isNaN(haversinKilometers(1, 1, Double.NaN, 1)));
    assertTrue(Double.isNaN(haversinKilometers(1, Double.NaN, 1, 1)));
    assertTrue(Double.isNaN(haversinKilometers(Double.NaN, 1, 1, 1)));

    assertEquals(0, haversinKilometers(0, 0, 0, 0), 0D);
    assertEquals(0, haversinKilometers(0, -180, 0, -180), 0D);
    assertEquals(0, haversinKilometers(0, -180, 0, 180), 0D);
    assertEquals(0, haversinKilometers(0, 180, 0, 180), 0D);
    assertEquals(0, haversinKilometers(90, 0, 90, 0), 0D);
    assertEquals(0, haversinKilometers(90, -180, 90, -180), 0D);
    assertEquals(0, haversinKilometers(90, -180, 90, 180), 0D);
    assertEquals(0, haversinKilometers(90, 180, 90, 180), 0D);

    // Test half a circle on the equator, using WGS84 mean earth radius in meters
    double earthRadiusMs = 6_371_008.7714;
    double halfCircle = earthRadiusMs * Math.PI / 1000;
    assertEquals(halfCircle, haversinKilometers(0, 0, 0, 180), 0D);

    Random r = random();
    double randomLat1 = 40.7143528 + (r.nextInt(10) - 5) * 360;
    double randomLon1 = -74.0059731 + (r.nextInt(10) - 5) * 360;

    double randomLat2 = 40.65 + (r.nextInt(10) - 5) * 360;
    double randomLon2 = -73.95 + (r.nextInt(10) - 5) * 360;

    assertEquals(
        8.5721137, haversinKilometers(randomLat1, randomLon1, randomLat2, randomLon2), 0.01D);

    // from solr and ES tests (with their respective epsilons)
    assertEquals(0, haversinKilometers(40.7143528, -74.0059731, 40.7143528, -74.0059731), 0D);
    assertEquals(
        5.28589, haversinKilometers(40.7143528, -74.0059731, 40.759011, -73.9844722), 0.01D);
    assertEquals(
        0.46210, haversinKilometers(40.7143528, -74.0059731, 40.718266, -74.007819), 0.01D);
    assertEquals(
        1.05498, haversinKilometers(40.7143528, -74.0059731, 40.7051157, -74.0088305), 0.01D);
    assertEquals(1.25812, haversinKilometers(40.7143528, -74.0059731, 40.7247222, -74), 0.01D);
    assertEquals(
        2.02852, haversinKilometers(40.7143528, -74.0059731, 40.731033, -73.9962255), 0.01D);
    assertEquals(8.57211, haversinKilometers(40.7143528, -74.0059731, 40.65, -73.95), 0.01D);
  }
}
