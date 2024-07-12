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
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ByteMultiVectorValue;

/**
 * A field that contains one or more byte numeric vectors for each document. Similar to {@link
 * KnnByteVectorField}, vectors are dense - that is, every dimension of a vector contains an
 * explicit value, stored packed into an array (of type byte[]) whose length is the vector
 * dimension.
 *
 * <p>All vectors in the field are required to have the same dimension, although different documents
 * can have different number of vectors. The {@link MultiVectorSimilarityFunction} may be used to
 * compare multi-vectors at query time, or during indexing for generating a nearest neighbour graph
 * (such as the HNSW graph).
 *
 * @lucene.experimental
 */
public class KnnByteMultiVectorField extends Field {

  private static FieldType createType(
      List<byte[]> t, VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
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
    type.setMultiVectorAttributes(dimension, VectorEncoding.BYTE, similarityFunction, aggregation);
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
    return new KnnByteVectorQuery(field, queryVector, k);
  }

  /**
   * Creates a byte numeric multi-vector field. Multi-vectors of a single field share the same
   * dimension and similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @param similarityFunction a {@link VectorSimilarityFunction} defining multi-vector proximity.
   * @param aggregation a {@link Aggregation} defining similarity aggregation across multiple vector
   *     values
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteMultiVectorField(
      String name,
      List<byte[]> value,
      VectorSimilarityFunction similarityFunction,
      Aggregation aggregation) {
    super(name, createType(value, similarityFunction, aggregation));
    assert type.vectorDimension() == value.get(0).length;
    fieldsData = new ByteMultiVectorValue(value, type.vectorDimension());
  }

  /**
   * Creates a byte numeric multi-vector field with the default EUCLIDEAN (L2) similarity and
   * default SUM_MAX aggregation. Vectors within a single multi-vector field share the same
   * dimension and similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteMultiVectorField(String name, List<byte[]> value) {
    this(
        name,
        value,
        VectorSimilarityFunction.EUCLIDEAN,
        MultiVectorSimilarityFunction.Aggregation.SUM_MAX);
  }

  /**
   * Creates a byte numeric multi-vector field. Vectors of a single multi-vector share the same
   * dimension and similarity function.
   *
   * @param name field name
   * @param value multi-vector value
   * @param fieldType field type
   * @throws IllegalArgumentException if any parameter is null, or the vector is empty or has
   *     dimension &gt; 1024.
   */
  public KnnByteMultiVectorField(String name, List<byte[]> value, FieldType fieldType) {
    super(name, fieldType);
    if (fieldType.vectorEncoding() != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "Attempt to create a multi-vector for field "
              + name
              + " using List<byte[]> but the field encoding is "
              + fieldType.vectorEncoding());
    }
    Objects.requireNonNull(value, "multi-vector value must not be null");
    checkDimensions(value, fieldType.vectorDimension());
    fieldsData = new ByteMultiVectorValue(value, fieldType.vectorDimension());
  }

  /** Return the multi-vector value of this field */
  public ByteMultiVectorValue value() {
    return (ByteMultiVectorValue) fieldsData;
  }

  /**
   * Set the multi-vector value of this field
   *
   * @param value the value to set; must not be null, and dimension must match the field type
   */
  public void setValue(ByteMultiVectorValue value) {
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

  private static void checkDimensions(List<byte[]> value, int dimensions) {
    for (byte[] val : value) {
      if (val.length != dimensions) {
        throw new IllegalArgumentException(
            "All vectors in the multi-vector should have the "
                + "same dimension value as configured in the fieldType");
      }
    }
  }
}
