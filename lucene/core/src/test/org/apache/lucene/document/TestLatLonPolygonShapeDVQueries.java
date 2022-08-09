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

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.IndexReader;

/** Test queries over LatLon Polygon ShapeDocValues */
public class TestLatLonPolygonShapeDVQueries extends BaseLatLonShapeDocValueTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Field[] createIndexableFields(String field, Object shape) {
    Polygon polygon = (Polygon) shape;
    Field[] fields = new Field[1];
    fields[0] = LatLonShape.createDocValueField(FIELD_NAME, polygon);
    return fields;
  }

  @Override
  protected Validator getValidator() {
    return new TestLatLonPolygonShapeQueries.PolygonValidator(this.ENCODER);
  }

  /** test random line queries */
  @Override
  protected void verifyRandomLineQueries(IndexReader reader, Object... shapes) throws Exception {
    // NOT IMPLEMENTED YET
  }

  /** test random polygon queries */
  @Override
  protected void verifyRandomPolygonQueries(IndexReader reader, Object... shapes) throws Exception {
    // NOT IMPLEMENTED YET
  }

  /** test random point queries */
  @Override
  protected void verifyRandomPointQueries(IndexReader reader, Object... shapes) throws Exception {
    // NOT IMPLEMENTED YET
  }

  /** test random distance queries */
  @Override
  protected void verifyRandomDistanceQueries(IndexReader reader, Object... shapes)
      throws Exception {
    // NOT IMPLEMENTED YET
  }
}
