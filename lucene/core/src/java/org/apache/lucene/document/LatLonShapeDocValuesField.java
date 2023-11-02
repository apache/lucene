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
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;

/**
 * Concrete implementation of a {@link ShapeDocValuesField} for geographic geometries.
 *
 * <p>This field should be instantiated through {@link LatLonShape#createDocValueField(String,
 * Line)}
 *
 * <ul>
 *   <li>{@link LatLonShape#createDocValueField(String, Polygon)} for indexing a geographic polygon
 *       doc value field.
 *   <li>{@link LatLonShape#createDocValueField(String, Line)} for indexing a geographic linestring
 *       doc value.
 *   <li>{@link LatLonShape#createDocValueField(String, double, double)} for indexing a lat, lon
 *       geographic point doc value.
 *   <li>{@link LatLonShape#createDocValueField(String, List)} for indexing a geographic doc value
 *       from a precomputed tessellation.
 *   <li>{@link LatLonShape#createDocValueField(String, BytesRef)} for indexing a geographic doc
 *       value from existing encoding.
 * </ul>
 *
 * <b>WARNING</b>: Like {@link LatLonShape}, vertex values are indexed with some loss of precision
 * from the original {@code double} values.
 *
 * @see PointValues
 * @see LatLonDocValuesField
 * @lucene.experimental
 */
public final class LatLonShapeDocValuesField extends ShapeDocValuesField {
  /** constructs a {@code LatLonShapeDocValueField} from a pre-tessellated geometry */
  protected LatLonShapeDocValuesField(String name, List<ShapeField.DecodedTriangle> tessellation) {
    super(name, new LatLonShapeDocValues(tessellation));
  }

  /** Creates a {@code LatLonShapeDocValueField} from a given serialized value */
  protected LatLonShapeDocValuesField(String name, BytesRef binaryValue) {
    super(name, new LatLonShapeDocValues(binaryValue));
  }

  /** retrieves the centroid location for the geometry */
  @Override
  public Point getCentroid() {
    return (Point) shapeDocValues.getCentroid();
  }

  /** retrieves the bounding box for the geometry */
  @Override
  public Rectangle getBoundingBox() {
    return (Rectangle) shapeDocValues.getBoundingBox();
  }

  @Override
  protected double decodeX(int encoded) {
    return GeoEncodingUtils.decodeLongitude(encoded);
  }

  @Override
  protected double decodeY(int encoded) {
    return GeoEncodingUtils.decodeLatitude(encoded);
  }
}
