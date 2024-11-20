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

import java.util.Arrays;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.Query;

/** Base test class for XYShape doc values */
public abstract class BaseXYShapeDocValueTestCase extends BaseXYShapeTestCase {
  @Override
  protected ShapeField.QueryRelation[] getSupportedQueryRelations() {
    return new ShapeField.QueryRelation[] {
      ShapeField.QueryRelation.INTERSECTS,
      ShapeField.QueryRelation.WITHIN,
      ShapeField.QueryRelation.DISJOINT
    };
  }

  @Override
  protected Query newRectQuery(
      String field,
      ShapeField.QueryRelation queryRelation,
      double minX,
      double maxX,
      double minY,
      double maxY) {
    return XYShape.newSlowDocValuesBoxQuery(
        field, queryRelation, (float) minX, (float) maxX, (float) minY, (float) maxY);
  }

  @Override
  protected Query newLineQuery(
      String field, ShapeField.QueryRelation queryRelation, Object... lines) {
    return ShapeDocValuesField.newGeometryQuery(
        field, queryRelation, (Object) Arrays.stream(lines).toArray(XYLine[]::new));
  }

  @Override
  protected Query newPolygonQuery(
      String field, ShapeField.QueryRelation queryRelation, Object... polygons) {
    return ShapeDocValuesField.newGeometryQuery(
        field, queryRelation, (Object) Arrays.stream(polygons).toArray(XYPolygon[]::new));
  }

  @Override
  protected Query newPointsQuery(
      String field, ShapeField.QueryRelation queryRelation, Object... points) {
    return ShapeDocValuesField.newGeometryQuery(
        field, queryRelation, (Object) Arrays.stream(points).toArray(float[][]::new));
  }

  @Override
  protected Query newDistanceQuery(
      String field, ShapeField.QueryRelation queryRelation, Object circle) {
    return LatLonShape.newDistanceQuery(field, queryRelation, (Circle) circle);
  }
}
