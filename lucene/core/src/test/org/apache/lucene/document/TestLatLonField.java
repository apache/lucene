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
package org.apache.lucene.document;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Simple tests for {@link LatLonPoint} TODO: move this lone test and remove class? */
public class TestLatLonField extends LuceneTestCase {

  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals(
        "LatLonField <field:18.313693958334625,-65.22744401358068>",
        (new LatLonField("field", 18.313694, -65.227444)).toString());

    Query query =
        LatLonField.newGeometryQuery(
            "field", ShapeField.QueryRelation.INTERSECTS, new Rectangle(18, 19, -66, -65));

    assertTrue(query instanceof IndexOrDocValuesQuery);

    IndexOrDocValuesQuery indexOrDocValuesQuery = (IndexOrDocValuesQuery) query;

    // looks crazy due to lossiness
    assertEquals(
        "field:[18.000000016763806 TO 18.999999999068677],[-65.9999999217689 TO -65.00000006519258]",
        indexOrDocValuesQuery.getIndexQuery().toString());
    assertEquals(
        "field:box(minLat=18.000000016763806, maxLat=18.999999999068677, minLon=-65.9999999217689, maxLon=-65.00000006519258)",
        indexOrDocValuesQuery.getRandomAccessQuery().toString());

    assertEquals(
        "(LatLonPointDistanceFeatureQuery(field=,originLat=18.0,originLon=-66.0,pivotDistance=50.0))^2.0",
        LatLonField.newDistanceFeatureQuery("field", 2.0f, 18, -66, 50).toString());

    assertEquals(
        "<distance:\"field\" latitude=2.0 longitude=18.0>",
        LatLonField.newDistanceSort("field", 2.0f, 18).toString());
  }
}
