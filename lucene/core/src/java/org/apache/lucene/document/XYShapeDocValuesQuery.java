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

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.util.BytesRef;

/**
 * Bounding Box query for {@link ShapeDocValuesField} representing {@link XYShape}
 *
 * @lucene.experimental
 */
final class XYShapeDocValuesQuery extends BaseShapeDocValuesQuery {
  XYShapeDocValuesQuery(
      String field, ShapeField.QueryRelation queryRelation, XYGeometry... geometries) {
    super(field, queryRelation, geometries);
  }

  @Override
  protected Component2D createComponent2D(Geometry... geometries) {
    return XYGeometry.create((XYGeometry[]) geometries);
  }

  @Override
  protected ShapeDocValues getShapeDocValues(BytesRef binaryValue) {
    return new XYShapeDocValues(binaryValue);
  }

  @Override
  protected SpatialVisitor getSpatialVisitor() {
    return XYShapeQuery.getSpatialVisitor(queryComponent2D);
  }
}
