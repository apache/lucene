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
import org.apache.lucene.search.KnnByteTensorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ByteMultiVectorValue;

/**
 * A field that contains one or more byte numeric vectors for each document. Similar to {@link
 * KnnByteVectorField}, vectors are dense - that is, every dimension of a vector contains an
 * explicit value, stored packed into an array (of type byte[]) whose length is the vector
 * dimension.
 *
 * <p>Only rank 2 tensors are currently supported. All vectors in a tensor field are required to
 * have the same dimension, although different documents can have different number of vectors. The
 * {@link MultiVectorSimilarityFunction} may be used to compare tensors at query time, or during indexing
 * for generating a nearest neighbour graph (such as the HNSW graph).
 *
 * @lucene.experimental
 */
public class KnnByteTensorField extends Field {

  private static FieldType createType(
      List<byte[]> t, VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
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
    type.setTensorAttributes(true, dimension, VectorEncoding.BYTE, similarityFunction, aggregation);
    type.freeze();
    return type;
  }

  /**
   * A convenience method for creating a tensor field type.
   *
   * @param dimension Dimension of vectors in the tensor
   * @param similarityFunction a function to compute similarity between two tensors
   * @throws IllegalArgumentException if any parameter is null, or has dimension &gt; 1024.
   */
  public static FieldType createFieldType(
      int dimension, MultiVectorSimilarityFunction similarityFunction) {
    FieldType type = new FieldType();
    type.setTensorAttributes(
        true,
        dimension,
        VectorEncoding.BYTE,
        similarityFunction.similarityFunction,
        similarityFunction.aggregation);
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
    return KnnByteTensorQuery.create(field, queryVector, k);
  }

  /**
   * Creates a byte numeric tensor field. Fields are multi-valued: each document has one or more
   * values. Tensors of a single field share the same dimension and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param similarityFunction a {@link VectorSimilarityFunction} defining tensor proximity.
   * @param aggregation a {@link Aggregation} defining similarity aggregation across multiple vector
   *     values
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteTensorField(
      String name,
      List<byte[]> tensor,
      VectorSimilarityFunction similarityFunction,
      Aggregation aggregation) {
    super(name, createType(tensor, similarityFunction, aggregation));
    assert type.vectorDimension() == tensor.get(0).length;
    fieldsData = new ByteMultiVectorValue(tensor, type.vectorDimension());
  }

  /**
   * Creates a byte numeric tensor field with the default EUCLIDEAN (L2) similarity and default
   * SUM_MAX aggregation. Vectors within a single tensor field share the same dimension and
   * similarity function.
   *
   * @param name field name
   * @param tensor value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteTensorField(String name, List<byte[]> tensor) {
    this(name, tensor, VectorSimilarityFunction.EUCLIDEAN, Aggregation.SUM_MAX);
  }

  /**
   * Creates a byte numeric tensor field. Vectors of a single tensor share the same dimension and
   * similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteTensorField(String name, List<byte[]> tensor, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.vectorEncoding() != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "Attempt to create a tensor for field "
              + name
              + " using List<byte[]> but the field encoding is "
              + fieldType.vectorEncoding());
    }
    Objects.requireNonNull(tensor, "tensor value must not be null");
    checkDimensions(tensor, fieldType.vectorDimension());
    fieldsData = new ByteMultiVectorValue(tensor, fieldType.vectorDimension());
  }

  /** Return the tensor value of this field */
  public ByteMultiVectorValue tensorValue() {
    return (ByteMultiVectorValue) fieldsData;
  }

  /**
   * Set the tensor value of this field
   *
   * @param value the value to set; must not be null, and dimension must match the field type
   */
  public void setTensorValue(ByteMultiVectorValue value) {
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

  private static void checkDimensions(List<byte[]> tensor, int dimensions) {
    for (byte[] val : tensor) {
      if (val.length != dimensions) {
        throw new IllegalArgumentException(
            "All vectors in the tensor should have the "
                + "same dimension value as configured in the fieldType");
      }
    }
  }
}
