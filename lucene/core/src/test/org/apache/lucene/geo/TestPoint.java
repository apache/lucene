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

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestPoint extends LuceneTestCase {

  public void testInvalidLat() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new Point(134.14, 45.23);
            });
    assertTrue(
        expected.getMessage().contains("invalid latitude 134.14; must be between -90.0 and 90.0"));
  }

  public void testInvalidLon() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new Point(43.5, 180.5);
            });
    assertTrue(
        expected
            .getMessage()
            .contains("invalid longitude 180.5; must be between -180.0 and 180.0"));
  }

  public void testEqualsAndHashCode() {
    Point point = GeoTestUtil.nextPoint();
    Point copy = new Point(point.getLat(), point.getLon());

    assertEquals(point, copy);
    assertEquals(point.hashCode(), copy.hashCode());

    Point otherPoint = GeoTestUtil.nextPoint();
    if (Double.compare(point.getLat(), otherPoint.getLat()) != 0
        || Double.compare(point.getLon(), otherPoint.getLon()) != 0) {
      assertNotEquals(point, otherPoint);
      // it is possible to have hashcode collisions
    } else {
      assertEquals(point, otherPoint);
      assertEquals(point.hashCode(), otherPoint.hashCode());
    }
  }
}
