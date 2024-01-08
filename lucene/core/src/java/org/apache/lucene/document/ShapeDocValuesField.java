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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.ShapeField.DecodedTriangle.TYPE;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;

/**
 * A doc values field for {@link LatLonShape} and {@link XYShape} that uses {@link ShapeDocValues}
 * as the underlying binary doc value format.
 *
 * <p>Note that this class cannot be instantiated directly due to different encodings {@link
 * org.apache.lucene.geo.XYEncodingUtils} and {@link org.apache.lucene.geo.GeoEncodingUtils}
 *
 * <p>Concrete Implementations include: {@link LatLonShapeDocValuesField} and {@link
 * XYShapeDocValuesField}
 *
 * @lucene.experimental
 */
public abstract class ShapeDocValuesField extends Field {

  /** the binary doc value format for this field */
  protected final ShapeDocValues shapeDocValues;

  /** FieldType for ShapeDocValues field */
  protected static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.freeze();
  }

  ShapeDocValuesField(String name, ShapeDocValues shapeDocValues) {
    super(name, FIELD_TYPE);
    this.shapeDocValues = shapeDocValues;
    this.fieldsData = shapeDocValues.binaryValue();
  }

  /** The name of the field */
  @Override
  public String name() {
    return name;
  }

  /** Gets the {@code IndexableFieldType} for this ShapeDocValue field */
  @Override
  public IndexableFieldType fieldType() {
    return FIELD_TYPE;
  }

  /** Currently there is no string representation for the ShapeDocValueField */
  @Override
  public String stringValue() {
    return null;
  }

  /** TokenStreams are not yet supported */
  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    return null;
  }

  /** Returns the number of terms (tessellated triangles) for this shape */
  public int numberOfTerms() {
    return shapeDocValues.numberOfTerms();
  }

  /** Creates a geometry query for shape docvalues */
  public static Query newGeometryQuery(
      final String field, final QueryRelation relation, Object... geometries) {
    // TODO add general geometry query
    throw new IllegalStateException(
        "geometry queries not yet supported on shape doc values for field [" + field + "]");
  }

  /** retrieves the centroid location for the geometry */
  public abstract Geometry getCentroid();

  /** retrieves the bounding box for the geometry */
  public abstract Geometry getBoundingBox();

  /**
   * Retrieves the highest dimensional type (POINT, LINE, TRIANGLE) for computing the geometry(s)
   * centroid
   */
  public TYPE getHighestDimensionType() {
    return shapeDocValues.getHighestDimension();
  }

  /** decodes x coordinates from encoded space */
  protected abstract double decodeX(int encoded);

  /** decodes y coordinates from encoded space */
  protected abstract double decodeY(int encoded);
}
