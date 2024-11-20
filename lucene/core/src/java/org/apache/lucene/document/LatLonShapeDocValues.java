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
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.util.BytesRef;

/**
 * A concrete implementation of {@link ShapeDocValues} for storing binary doc value representation
 * of {@link LatLonShape} geometries in a {@link LatLonShapeDocValuesField}
 *
 * <p>Note: This class cannot be instantiated directly. See {@link LatLonShape} for factory API
 * based on different geometries.
 *
 * @lucene.experimental
 */
public final class LatLonShapeDocValues extends ShapeDocValues {
  /** protected ctor for instantiating a lat lon doc value based on a tessellation */
  protected LatLonShapeDocValues(List<ShapeField.DecodedTriangle> tessellation) {
    super(tessellation);
  }

  /**
   * protected ctor for instantiating a lat lon doc value based on an already retrieved binary
   * format
   */
  protected LatLonShapeDocValues(BytesRef binaryValue) {
    super(binaryValue);
  }

  @Override
  public Point getCentroid() {
    return (Point) centroid;
  }

  @Override
  public Rectangle getBoundingBox() {
    return (Rectangle) boundingBox;
  }

  @Override
  protected Point computeCentroid() {
    Encoder encoder = getEncoder();
    return new Point(
        encoder.decodeY(getEncodedCentroidY()), encoder.decodeX(getEncodedCentroidX()));
  }

  @Override
  protected Rectangle computeBoundingBox() {
    Encoder encoder = getEncoder();
    return new Rectangle(
        encoder.decodeY(getEncodedMinY()), encoder.decodeY(getEncodedMaxY()),
        encoder.decodeX(getEncodedMinX()), encoder.decodeX(getEncodedMaxX()));
  }

  @Override
  protected Encoder getEncoder() {
    return new Encoder() {
      @Override
      public int encodeX(double x) {
        return GeoEncodingUtils.encodeLongitude(x);
      }

      @Override
      public int encodeY(double y) {
        return GeoEncodingUtils.encodeLatitude(y);
      }

      @Override
      public double decodeX(int encoded) {
        return GeoEncodingUtils.decodeLongitude(encoded);
      }

      @Override
      public double decodeY(int encoded) {
        return GeoEncodingUtils.decodeLatitude(encoded);
      }
    };
  }
}
