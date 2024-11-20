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

import java.util.List;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;

/**
 * Concrete implementation of a {@link ShapeDocValuesField} for cartesian geometries.
 *
 * <p>This field should be instantiated through {@link XYShape#createDocValueField(String, XYLine)}
 *
 * <ul>
 *   <li>{@link XYShape#createDocValueField(String, XYPolygon)} for indexing a cartesian polygon doc
 *       value field.
 *   <li>{@link XYShape#createDocValueField(String, XYLine)} for indexing a cartesian linestring doc
 *       value.
 *   <li>{@link XYShape#createDocValueField(String, float, float)} for indexing a x, y cartesian
 *       point doc value.
 *   <li>{@link XYShape#createDocValueField(String, List)} for indexing a cartesian doc value from a
 *       precomputed tessellation.
 *   <li>{@link XYShape#createDocValueField(String, BytesRef)} for indexing a cartesian doc value
 *       from existing encoding.
 * </ul>
 *
 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision
 * from the original {@code double} values.
 *
 * @see PointValues
 * @see XYDocValuesField
 * @lucene.experimental
 */
public final class XYShapeDocValuesField extends ShapeDocValuesField {

  /** constructs a {@code XYShapeDocValueField} from a pre-tessellated geometry */
  protected XYShapeDocValuesField(String name, List<ShapeField.DecodedTriangle> tessellation) {
    super(name, new XYShapeDocValues(tessellation));
  }

  /** Creates a {@code XYShapeDocValueField} from a given serialized value */
  protected XYShapeDocValuesField(String name, BytesRef binaryValue) {
    super(name, new XYShapeDocValues(binaryValue));
  }

  /** retrieves the centroid location for the geometry */
  @Override
  public XYPoint getCentroid() {
    return (XYPoint) shapeDocValues.getCentroid();
  }

  @Override
  public XYRectangle getBoundingBox() {
    return (XYRectangle) shapeDocValues.getBoundingBox();
  }

  @Override
  protected double decodeX(int encoded) {
    return XYEncodingUtils.decode(encoded);
  }

  @Override
  protected double decodeY(int encoded) {
    return XYEncodingUtils.decode(encoded);
  }
}
