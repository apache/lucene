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

import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * A field that contains a single float16 numeric vector (or none) for each document. Vectors are
 * dense - that is, every dimension of a vector contains an explicit value, stored packed into an
 * array (of type short[]) whose length is the vector dimension. Values can be retrieved using
 * {@link Float16VectorValues}, which is a forward-only docID-based iterator and also offers
 * random-access by dense ordinal (not docId). {@link VectorSimilarityFunction} may be used to
 * compare vectors at query time (for example as part of result ranking). A {@link
 * KnnFloat16VectorField} is associated with a search similarity function defining the metric used
 * for nearest-neighbor search among vectors of that field.
 *
 * @lucene.experimental
 */
public class KnnFloat16VectorField extends Field {

  private static FieldType createType(short[] v, VectorSimilarityFunction similarityFunction) {
    if (v == null) {
      throw new IllegalArgumentException("vector value must not be null");
    }
    int dimension = v.length;
    if (dimension == 0) {
      throw new IllegalArgumentException("cannot index an empty vector");
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("similarity function must not be null");
    }
    FieldType type = new FieldType();
    type.setVectorAttributes(dimension, VectorEncoding.FLOAT16, similarityFunction);
    type.freeze();
    return type;
  }

  /**
   * Creates a numeric vector field. Fields are single-valued: each document has either one value or
   * no value. Vectors of a single field share the same dimension and similarity function.
   *
   * @param name field name
   * @param vector value
   * @param similarityFunction a function defining vector proximity.
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty
   */
  public KnnFloat16VectorField(
      String name, short[] vector, VectorSimilarityFunction similarityFunction) {
    super(name, createType(vector, similarityFunction));
    fieldsData = VectorUtil.checkFiniteFloat16(vector); // null check done above
  }

  /** Return the vector value of this field */
  public short[] vectorValue() {
    return (short[]) fieldsData;
  }

  /**
   * Set the vector value of this field
   *
   * @param value the value to set; must not be null, and length must match the field type
   */
  public void setVectorValue(short[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (value.length != type.vectorDimension()) {
      throw new IllegalArgumentException(
          "value length " + value.length + " must match field dimension " + type.vectorDimension());
    }
    fieldsData = VectorUtil.checkFiniteFloat16(value);
  }
}
