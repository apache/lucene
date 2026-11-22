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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A {@link DoubleValuesSource} that computes vector similarity between a query vector and the raw
 * full-precision vectors indexed in the provided {@link
 * org.apache.lucene.document.KnnFloatVectorField} in documents.
 */
public class FullPrecisionFloatVectorSimilarityValuesSource
    extends AbstractFullPrecisionVectorSimilarityValuesSource {

  private final float[] queryVector;

  /**
   * Creates a {@link DoubleValuesSource} that returns the vector similarity score between the
   * provided query vector and the field for documents.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   * @param vectorSimilarityFunction the vector similarity function to use
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(
      float[] vector, String fieldName, VectorSimilarityFunction vectorSimilarityFunction) {
    super(fieldName, vectorSimilarityFunction);
    this.queryVector = vector;
  }

  /**
   * Creates a {@link DoubleValuesSource} that returns the vector similarity score between the
   * provided query vector and the field for documents, using the similarity function configured for
   * the field.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(float[] vector, String fieldName) {
    this(vector, fieldName, null);
  }

  @Override
  protected KnnVectorValues getVectorValues(LeafReaderContext ctx) throws IOException {
    return ctx.reader().getFloatVectorValues(fieldName);
  }

  @Override
  protected void checkField(LeafReaderContext ctx) {
    FloatVectorValues.checkField(ctx.reader(), fieldName);
  }

  @Override
  protected int queryDimension() {
    return queryVector.length;
  }

  @Override
  protected VectorScorer fullPrecisionRescorer(KnnVectorValues vectorValues) throws IOException {
    return ((FloatVectorValues) vectorValues).rescorer(queryVector);
  }

  @Override
  protected double compareToQuery(KnnVectorValues vectorValues, int ord) throws IOException {
    return vectorSimilarityFunction.compare(
        queryVector, ((FloatVectorValues) vectorValues).vectorValue(ord));
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector), vectorSimilarityFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FullPrecisionFloatVectorSimilarityValuesSource other =
        (FullPrecisionFloatVectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && Objects.equals(vectorSimilarityFunction, other.vectorSimilarityFunction)
        && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "FullPrecisionFloatVectorSimilarityValuesSource(fieldName="
        + fieldName
        + " vectorSimilarityFunction="
        + vectorSimilarityFunction
        + " queryVector="
        + Arrays.toString(queryVector)
        + ")";
  }
}
