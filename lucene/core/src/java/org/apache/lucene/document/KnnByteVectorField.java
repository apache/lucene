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

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;

/**
 * A field that contains a single byte numeric vector (or none) for each document. Vectors are dense
 * - that is, every dimension of a vector contains an explicit value, stored packed into an array
 * (of type byte[]) whose length is the vector dimension. Values can be retrieved using {@link
 * ByteVectorValues}, which is a forward-only docID-based iterator and also offers random-access by
 * dense ordinal (not docId). {@link VectorSimilarityFunction} may be used to compare vectors at
 * query time (for example as part of result ranking). A KnnByteVectorField may be associated with a
 * search similarity function defining the metric used for nearest-neighbor search among vectors of
 * that field.
 *
 * @lucene.experimental
 */
public class KnnByteVectorField extends Field {

  private static FieldType createType(byte[] v, VectorSimilarityFunction similarityFunction) {
    if (v == null) {
      throw new IllegalArgumentException("vector value must not be null");
    }
    int dimension = v.length;
    if (dimension == 0) {
      throw new IllegalArgumentException("cannot index an empty vector");
    }
    if (dimension > ByteVectorValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException(
          "cannot index vectors with dimension greater than " + ByteVectorValues.MAX_DIMENSIONS);
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("similarity function must not be null");
    }
    FieldType type = new FieldType();
    type.setVectorAttributes(dimension, VectorEncoding.BYTE, similarityFunction);
    type.freeze();
    return type;
  }

  /**
   * Create a new vector query for the provided field targeting the byte vector
   *
   * @param field The field to query
   * @param queryVector The byte vector target
   * @param k The number of nearest neighbors to gather
   * @return A new vector query
   */
  public static Query newVectorQuery(String field, byte[] queryVector, int k) {
    return new KnnByteVectorQuery(field, queryVector, k);
  }

  /**
   * A convenience method for creating a vector field type.
   *
   * @param dimension dimension of vectors
   * @param similarityFunction a function defining vector proximity.
   * @throws IllegalArgumentException if any parameter is null, or has dimension &gt; 1024.
   */
  public static FieldType createFieldType(
      int dimension, VectorSimilarityFunction similarityFunction) {
    FieldType type = new FieldType();
    type.setVectorAttributes(dimension, VectorEncoding.BYTE, similarityFunction);
    type.freeze();
    return type;
  }

  /**
   * Creates a numeric vector field. Fields are single-valued: each document has either one value or
   * no value. Vectors of a single field share the same dimension and similarity function. Note that
   * some vector similarities (like {@link VectorSimilarityFunction#DOT_PRODUCT}) require values to
   * be constant-length.
   *
   * @param name field name
   * @param vector value
   * @param similarityFunction a function defining vector proximity.
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteVectorField(
      String name, byte[] vector, VectorSimilarityFunction similarityFunction) {
    super(name, createType(vector, similarityFunction));
    fieldsData = vector;
  }

  /**
   * Creates a numeric vector field with the default EUCLIDEAN_HNSW (L2) similarity. Fields are
   * single-valued: each document has either one value or no value. Vectors of a single field share
   * the same dimension and similarity function.
   *
   * @param name field name
   * @param vector value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteVectorField(String name, byte[] vector) {
    this(name, vector, VectorSimilarityFunction.EUCLIDEAN);
  }

  /**
   * Creates a numeric vector field. Fields are single-valued: each document has either one value or
   * no value. Vectors of a single field share the same dimension and similarity function.
   *
   * @param name field name
   * @param vector value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteVectorField(String name, byte[] vector, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.vectorEncoding() != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "Attempt to create a vector for field "
              + name
              + " using byte[] but the field encoding is "
              + fieldType.vectorEncoding());
    }
    fieldsData = vector;
  }

  /** Return the vector value of this field */
  public byte[] vectorValue() {
    return (byte[]) fieldsData;
  }

  /**
   * Set the vector value of this field
   *
   * @param value the value to set; must not be null, and length must match the field type
   */
  public void setVectorValue(byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (value.length != type.vectorDimension()) {
      throw new IllegalArgumentException(
          "value length " + value.length + " must match field dimension " + type.vectorDimension());
    }
    fieldsData = value;
  }
}
