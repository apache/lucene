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
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.util.BytesRef;

/**
 * A concrete implementation of {@link ShapeDocValues} for storing binary doc value representation
 * of {@link XYShape} geometries in a {@link XYShapeDocValuesField}
 *
 * <p>Note: This class cannot be instantiated directly. See {@link XYShape} for factory API based on
 * different geometries.
 *
 * @lucene.experimental
 */
public final class XYShapeDocValues extends ShapeDocValues {
  /** protected ctor for instantiating a cartesian doc value based on a tessellation */
  protected XYShapeDocValues(List<ShapeField.DecodedTriangle> tessellation) {
    super(tessellation);
  }

  /**
   * protected ctor for instantiating a cartesian doc value based on an already retrieved binary
   * format
   */
  protected XYShapeDocValues(BytesRef binaryValue) {
    super(binaryValue);
  }

  @Override
  public XYPoint getCentroid() {
    return (XYPoint) centroid;
  }

  @Override
  public XYRectangle getBoundingBox() {
    return (XYRectangle) boundingBox;
  }

  @Override
  protected XYPoint computeCentroid() {
    Encoder encoder = getEncoder();
    return new XYPoint(
        (float) encoder.decodeX(getEncodedCentroidX()),
        (float) encoder.decodeY(getEncodedCentroidY()));
  }

  @Override
  protected XYRectangle computeBoundingBox() {
    Encoder encoder = getEncoder();
    return new XYRectangle(
        (float) encoder.decodeX(getEncodedMinX()), (float) encoder.decodeX(getEncodedMaxX()),
        (float) encoder.decodeY(getEncodedMinY()), (float) encoder.decodeY(getEncodedMaxY()));
  }

  @Override
  protected Encoder getEncoder() {
    return new Encoder() {
      @Override
      public int encodeX(double x) {
        return XYEncodingUtils.encode((float) x);
      }

      @Override
      public int encodeY(double y) {
        return XYEncodingUtils.encode((float) y);
      }

      @Override
      public double decodeX(int encoded) {
        return XYEncodingUtils.decode(encoded);
      }

      @Override
      public double decodeY(int encoded) {
        return XYEncodingUtils.decode(encoded);
      }
    };
  }
}
