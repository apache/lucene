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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A {@link DoubleValuesSource} that computes vector similarity between a query vector and the raw
 * half-precision (FLOAT16) vectors indexed in the provided {@link
 * org.apache.lucene.document.KnnFloat16VectorField} in documents.
 */
public class FullPrecisionFloat16VectorSimilarityValuesSource
    extends AbstractFullPrecisionVectorSimilarityValuesSource {

  private final short[] queryVector;

  /**
   * Creates a {@link DoubleValuesSource} that returns the vector similarity score between the
   * provided query vector and the field for documents.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloat16VectorField}
   * @param vectorSimilarityFunction the vector similarity function to use
   */
  public FullPrecisionFloat16VectorSimilarityValuesSource(
      short[] vector, String fieldName, VectorSimilarityFunction vectorSimilarityFunction) {
    super(fieldName, vectorSimilarityFunction);
    this.queryVector = vector;
  }

  /**
   * Creates a {@link DoubleValuesSource} that returns the vector similarity score between the
   * provided query vector and the field for documents, using the similarity function configured for
   * the field.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloat16VectorField}
   */
  public FullPrecisionFloat16VectorSimilarityValuesSource(short[] vector, String fieldName) {
    this(vector, fieldName, null);
  }

  @Override
  protected KnnVectorValues getVectorValues(LeafReaderContext ctx) throws IOException {
    return ctx.reader().getFloat16VectorValues(fieldName);
  }

  @Override
  protected void checkField(LeafReaderContext ctx) {
    Float16VectorValues.checkField(ctx.reader(), fieldName);
  }

  @Override
  protected int queryDimension() {
    return queryVector.length;
  }

  @Override
  protected VectorScorer fullPrecisionRescorer(KnnVectorValues vectorValues) throws IOException {
    return ((Float16VectorValues) vectorValues).rescorer(queryVector);
  }

  @Override
  protected double compareToQuery(KnnVectorValues vectorValues, int ord) throws IOException {
    return vectorSimilarityFunction.compare(
        queryVector, ((Float16VectorValues) vectorValues).vectorValue(ord));
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector), vectorSimilarityFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FullPrecisionFloat16VectorSimilarityValuesSource other =
        (FullPrecisionFloat16VectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && Objects.equals(vectorSimilarityFunction, other.vectorSimilarityFunction)
        && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "FullPrecisionFloat16VectorSimilarityValuesSource(fieldName="
        + fieldName
        + " vectorSimilarityFunction="
        + vectorSimilarityFunction
        + " queryVector="
        + Arrays.toString(queryVector)
        + ")";
  }
}
