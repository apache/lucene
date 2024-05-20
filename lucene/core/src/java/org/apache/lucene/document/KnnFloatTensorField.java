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

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.VectorUtil;

import java.util.List;
import java.util.Objects;

/**
 * A field that contains one or more floating-point numeric vectors for each document.
 * Similar to {@link KnnFloatVectorField}, vectors are dense - that is, every dimension of a vector
 * contains an explicit value, stored packed into an array (of type float[]) whose length is
 * the vector dimension.
 * Only rank 2 tensors are currently supported. All vectors in a tensor field are required to have
 * the same dimension, although different documents can have different number of vectors.
 * The {@link TensorSimilarityFunction} may be used to compare tensors at query time, or during indexing
 * for generating a nearest neighbour graph (such as the HNSW graph).
 * TODO: Value retrieved from ?? what iterators are supported? random access?
 *
 * @lucene.experimental
 */
// * A field that contains a single floating-point numeric vector (or none) for each document. Vectors
// * are dense - that is, every dimension of a vector contains an explicit value, stored packed into
// * an array (of type float[]) whose length is the vector dimension. Values can be retrieved using
// * {@link FloatVectorValues}, which is a forward-only docID-based iterator and also offers
// * random-access by dense ordinal (not docId). {@link VectorSimilarityFunction} may be used to
// * compare vectors at query time (for example as part of result ranking). A {@link
// * KnnFloatTensorField} may be associated with a search similarity function defining the metric used
// * for nearest-neighbor search among vectors of that field.
// *
// * @lucene.experimental
// */
public class KnnFloatTensorField extends Field {

  private static final int rank = 2;

  private static FieldType createType(List<float[]> t, TensorSimilarityFunction similarityFunction) {
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
      throw new IllegalArgumentException("empty vector found at index 0. Tensor cannot have empty vectors");
    }
    int dimension = t.get(0).length;
    checkDimensions(t, dimension);
    FieldType type = new FieldType();
    type.setTensorAttributes(rank, dimension, VectorEncoding.FLOAT32, similarityFunction);
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
  public static FieldType createFieldType(int dimension, TensorSimilarityFunction similarityFunction) {
    FieldType type = new FieldType();
    type.setTensorAttributes(rank, dimension, VectorEncoding.FLOAT32, similarityFunction);
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
   * Creates a numeric tensor field. Fields are multi-valued: each document has one or more
   * values. Tensors of a single field share the same dimension and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param similarityFunction a function defining tensor proximity.
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(
      String name, List<float[]> tensor, TensorSimilarityFunction similarityFunction) {
    super(name, createType(tensor, similarityFunction));
    tensor.forEach(VectorUtil::checkFinite);
    fieldsData = tensor;
  }

  /**
   * Creates a numeric tensor field with the default EUCLIDEAN_HNSW (L2) similarity. Vectors within a single
   * tensor field share the same dimension and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(String name, List<float[]> tensor) {
    this(name, tensor, TensorSimilarityFunction.MAX_EUCLIDEAN);
  }

  /**
   * Creates a numeric tensor field. Vectors of a single tensor share the same dimension and similarity function.
   *
   * @param name field name
   * @param tensor value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnFloatTensorField(String name, List<float[]> tensor, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.tensorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Attempt to create a tensor for field "
              + name
              + " using List<float[]> but the field encoding is "
              + fieldType.vectorEncoding());
    }
    Objects.requireNonNull(tensor, "tensor value must not be null");
    if (fieldType.tensorRank() != rank) {
      throw new UnsupportedOperationException("Only tensors of rank = " + rank + "are supported");
    }
    checkDimensions(tensor, fieldType.tensorDimension());
    tensor.forEach(VectorUtil::checkFinite);
    fieldsData = tensor;
  }

  /** Return the tensor value of this field */
  public List<float[]> vectorValue() {
    return (List<float[]>) fieldsData;
  }

  /**
   * Set the tensor value of this field
   *
   * @param value the value to set; must not be null, and dimension must match the field type
   */
  public void setVectorValue(List<float[]> value) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("value must not be null or empty");
    }
    checkDimensions(value, type.tensorDimension());
    fieldsData = value;
  }

  private static void checkDimensions(List<float[]> tensor, int dimensions) {
    for(float[] val: tensor) {
      if (val.length != dimensions) {
        throw new IllegalArgumentException("All vectors in the tensor should have the " +
            "same dimension value as configured in the fieldType");
      }
    }
  }
}
