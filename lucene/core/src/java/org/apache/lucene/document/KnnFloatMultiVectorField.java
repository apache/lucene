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
import java.util.Objects;
import org.apache.lucene.index.MultiVectorSimilarityFunction;
import org.apache.lucene.index.MultiVectorSimilarityFunction.Aggregation;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FloatMultiVectorValue;
import org.apache.lucene.util.VectorUtil;

/**
 * A field that contains one or more floating-point numeric vectors for each document. Similar to
 * {@link KnnFloatVectorField}, vectors are dense - that is, every dimension of a vector contains an
 * explicit value, stored packed into an array (of type float[]) whose length is the vector
 * dimension.
 *
 * <p>All vectors in a multi-vector field are required to have the same dimension, although
 * different documents can have different number of vectors.
 *
 * <p>The {@link MultiVectorSimilarityFunction} may be used to compare multi-vectors at query time,
 * or during indexing for generating a nearest neighbour graph (such as the HNSW graph).
 *
 * @lucene.experimental
 */
public class KnnFloatMultiVectorField extends Field {

  private static FieldType createType(
      List<float[]> t, VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
    if (t == null) {
      throw new IllegalArgumentException("multi-vector value must not be null");
    }
    if (t.size() == 0) {
      throw new IllegalArgumentException("cannot index an empty multi-vector");
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("similarity function must not be null");
    }
    if (t.get(0).length == 0) {
      throw new IllegalArgumentException(
          "empty vector found at index 0. multi-vector cannot have empty vectors");
    }
    int dimension = t.get(0).length;
    checkDimensions(t, dimension);
    FieldType type = new FieldType();
    type.setMultiVectorAttributes(
        dimension, VectorEncoding.FLOAT32, similarityFunction, aggregation);
    type.freeze();
    return type;
  }

  /**
   * A convenience method for creating a multi-vector field type.
   *
   * @param dimension Dimension of vectors in the multi-vector
   * @param similarityFunction a function to compute similarity between two multi-vectors
   * @throws IllegalArgumentException if any parameter is null, or has dimension &gt; 1024.
   */
  public static FieldType createFieldType(
      int dimension, MultiVectorSimilarityFunction similarityFunction) {
    FieldType type = new FieldType();
    type.setMultiVectorAttributes(
        dimension,
        VectorEncoding.FLOAT32,
        similarityFunction.similarityFunction,
        similarityFunction.aggregation);
    type.freeze();
    return type;
  }

  /**
   * Create a new vector query for the provided field targeting the float vector
   *
   * @param field The field to query
   * @param queryVector The float vector target
   * @param k The number of nearest neighbors to gather
   * @return A new vector query
   */
  public static Query newVectorQuery(String field, float[] queryVector, int k) {
    return new KnnFloatVectorQuery(field, queryVector, k);
  }

  /**
   * Creates a numeric multi-vector field. Multi-vectors of a single field share the same dimension
   * and similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @param similarityFunction a {@link VectorSimilarityFunction} defining multi-vector proximity.
   * @param aggregation a {@link Aggregation} defining similarity aggregation across multiple vector
   *     values
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatMultiVectorField(
      String name,
      List<float[]> value,
      VectorSimilarityFunction similarityFunction,
      Aggregation aggregation) {
    super(name, createType(value, similarityFunction, aggregation));
    value.forEach(VectorUtil::checkFinite);
    assert type.vectorDimension() == value.get(0).length;
    fieldsData = new FloatMultiVectorValue(value, type.vectorDimension());
  }

  /**
   * Creates a numeric multi-vector field. Multi-vectors of a single field share the same dimension
   * and similarity function.
   *
   * @param name field name
   * @param value multi-vector
   * @param similarityFunction a {@link MultiVectorSimilarityFunction} defining multi-vector
   *     proximity.
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatMultiVectorField(
      String name, List<float[]> value, MultiVectorSimilarityFunction similarityFunction) {
    this(name, value, similarityFunction.similarityFunction, similarityFunction.aggregation);
  }

  /**
   * Creates a numeric multi-vector field with the default EUCLIDEAN_HNSW (L2) similarity and
   * SUM_MAX aggregation. Vectors within a single multi-vector field share the same dimension and
   * similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatMultiVectorField(String name, List<float[]> value) {
    this(
        name,
        value,
        VectorSimilarityFunction.EUCLIDEAN,
        MultiVectorSimilarityFunction.Aggregation.SUM_MAX);
  }

  /**
   * Creates a numeric multi-vector field. Vectors of a single multi-vector share the same dimension
   * and similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatMultiVectorField(String name, List<float[]> value, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.vectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Attempt to create a multi-vector for field "
              + name
              + " using List<float[]> but the field encoding is "
              + fieldType.vectorEncoding());
    }
    Objects.requireNonNull(value, "multi-vector value must not be null");
    checkDimensions(value, fieldType.vectorDimension());
    value.forEach(VectorUtil::checkFinite);
    fieldsData = new FloatMultiVectorValue(value, fieldType.vectorDimension());
  }

  /** Return the multi-vector value of this field */
  public FloatMultiVectorValue value() {
    return (FloatMultiVectorValue) fieldsData;
  }

  /**
   * Set the multi-vector value of this field
   *
   * @param value the value to set; must not be null, and dimension must match the field type
   */
  public void setValue(FloatMultiVectorValue value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null or empty");
    }
    if (value.dimension() != type.vectorDimension()) {
      throw new IllegalArgumentException(
          "value dimension ["
              + value.dimension()
              + "] "
              + "should match field multi-vector dimension: ["
              + type.vectorDimension()
              + "]");
    }
    fieldsData = value;
  }

  private static void checkDimensions(List<float[]> value, int dimensions) {
    for (float[] val : value) {
      if (val.length != dimensions) {
        throw new IllegalArgumentException(
            "All vectors in multi-vector value should have the "
                + "same dimension value as configured in the fieldType");
      }
    }
  }
}
