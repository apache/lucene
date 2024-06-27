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
import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.index.TensorSimilarityFunction.Aggregation;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnFloatTensorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FloatTensorValue;
import org.apache.lucene.util.VectorUtil;

/**
 * A field that contains one or more floating-point numeric vectors for each document. Similar to
 * {@link KnnFloatVectorField}, vectors are dense - that is, every dimension of a vector contains an
 * explicit value, stored packed into an array (of type float[]) whose length is the vector
 * dimension.
 *
 * <p>Only rank 2 tensors are currently supported. All vectors in a tensor field are
 * required to have the same dimension, although different documents can have different number of
 * vectors.</p>
 *
 * <p>The {@link TensorSimilarityFunction} may be used to compare tensors at query time, or
 * during indexing for generating a nearest neighbour graph (such as the HNSW graph).</p>
 *
 * @lucene.experimental
 */
public class KnnFloatTensorField extends Field {

  private static FieldType createType(
      List<float[]> t, VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
    if (t == null) {
      throw new IllegalArgumentException("tensor value must not be null");
    }
    if (t.size() == 0) {
      throw new IllegalArgumentException("cannot index an empty tensor");
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("similarity function must not be null");
    }
    if (t.get(0).length == 0) {
      throw new IllegalArgumentException(
          "empty vector found at index 0. Tensor cannot have empty vectors");
    }
    int dimension = t.get(0).length;
    checkDimensions(t, dimension);
    FieldType type = new FieldType();
    type.setTensorAttributes(
        true,
        dimension,
        VectorEncoding.FLOAT32,
        similarityFunction,
        aggregation);
    type.freeze();
    return type;
  }

  private static FieldType createType(
      List<float[]> t, TensorSimilarityFunction similarityFunction) {
    return createType(t, similarityFunction.similarityFunction, similarityFunction.aggregation);
  }

  /**
   * A convenience method for creating a tensor field type.
   *
   * @param dimension Dimension of vectors in the tensor
   * @param similarityFunction a function to compute similarity between two tensors
   * @throws IllegalArgumentException if any parameter is null, or has dimension &gt; 1024.
   */
  public static FieldType createFieldType(
      int dimension, TensorSimilarityFunction similarityFunction) {
    FieldType type = new FieldType();
    type.setTensorAttributes(
        true,
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
    return KnnFloatTensorQuery.create(field, queryVector, k);
  }

  /**
   * Creates a numeric tensor field. Fields are multi-valued: each document has one or more values.
   * Tensors of a single field share the same dimension and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param similarityFunction a {@link VectorSimilarityFunction} defining tensor proximity.
   * @param aggregation a {@link Aggregation} defining similarity aggregation across multiple vector values
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(
      String name, List<float[]> tensor, VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
    super(name, createType(tensor, similarityFunction, aggregation));
    tensor.forEach(VectorUtil::checkFinite);
    assert type.vectorDimension() == tensor.get(0).length;
    fieldsData = new FloatTensorValue(tensor, type.vectorDimension());
  }

  public KnnFloatTensorField(
      String name, List<float[]> tensor, TensorSimilarityFunction similarityFunction) {
    this(name, tensor, similarityFunction.similarityFunction, similarityFunction.aggregation);
  }

  /**
   * Creates a numeric tensor field with the default EUCLIDEAN_HNSW (L2) similarity
   * and SUM_MAX aggregation. Vectors within a single tensor field share the same dimension
   * and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(String name, List<float[]> tensor) {
    this(name, tensor, VectorSimilarityFunction.EUCLIDEAN, Aggregation.SUM_MAX);
  }

  /**
   * Creates a numeric tensor field. Vectors of a single tensor share the same dimension and
   * similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(String name, List<float[]> tensor, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.vectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Attempt to create a tensor for field "
              + name
              + " using List<float[]> but the field encoding is "
              + fieldType.vectorEncoding());
    }
    Objects.requireNonNull(tensor, "tensor value must not be null");
    checkDimensions(tensor, fieldType.vectorDimension());
    tensor.forEach(VectorUtil::checkFinite);
    fieldsData = new FloatTensorValue(tensor, fieldType.vectorDimension());
  }

  /** Return the tensor value of this field */
  public FloatTensorValue tensorValue() {
    return (FloatTensorValue) fieldsData;
  }

  /**
   * Set the tensor value of this field
   *
   * @param value the value to set; must not be null, and dimension must match the field type
   */
  public void setTensorValue(FloatTensorValue value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null or empty");
    }
    if (value.dimension() != type.vectorDimension()) {
      throw new IllegalArgumentException(
          "value dimension ["
              + value.dimension()
              + "] "
              + "should match field tensor dimension: ["
              + type.vectorDimension()
              + "]");
    }
    fieldsData = value;
  }

  private static void checkDimensions(List<float[]> tensor, int dimensions) {
    for (float[] val : tensor) {
      if (val.length != dimensions) {
        throw new IllegalArgumentException(
            "All vectors in the tensor should have the "
                + "same dimension value as configured in the fieldType");
      }
    }
  }
}
