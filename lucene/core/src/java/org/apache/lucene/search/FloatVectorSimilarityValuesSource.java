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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A {@link DoubleValuesSource} which computes the vector similarity scores between the query vector
 * and the {@link org.apache.lucene.document.KnnFloatVectorField} for documents.
 */
class FloatVectorSimilarityValuesSource extends VectorSimilarityValuesSource {

  /** Creates a {@link FloatVectorSimilarityValuesSource} that scores on full precision vector values */
  public static DoubleValues fullPrecisionScores(
      LeafReaderContext ctx, float[] queryVector, String vectorField) throws IOException {
    return new FloatVectorSimilarityValuesSource(queryVector, vectorField, true).getValues(ctx, null);
  }

  private final float[] queryVector;
  private final boolean useFullPrecision;

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents. Uses the scorer exposed by configured vectors reader.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   */
  public FloatVectorSimilarityValuesSource(float[] vector, String fieldName) {
    this(vector, fieldName, false);
  }

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   * @param useFullPrecision uses full precision raw vectors for similarity computation if true,
   *     otherwise the configured vectors reader is used, which may be quantized or full precision.
   */
  public FloatVectorSimilarityValuesSource(
      float[] vector, String fieldName, boolean useFullPrecision) {
    super(fieldName);
    this.queryVector = vector;
    this.useFullPrecision = useFullPrecision;
  }

  @Override
  public VectorScorer getScorer(LeafReaderContext ctx) throws IOException {
    final FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
    if (vectorValues == null) {
      FloatVectorValues.checkField(ctx.reader(), fieldName);
      return null;
    }

    final FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(fieldName);
    if (fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Field "
              + fieldName
              + " does not have the expected vector encoding: "
              + VectorEncoding.FLOAT32);
    }
    if (fi.getVectorDimension() != queryVector.length) {
      throw new IllegalArgumentException(
          "Query vector dimension does not match field dimension: "
              + queryVector.length
              + " != "
              + fi.getVectorDimension());
    }

    // default vector scorer
    if (useFullPrecision == false) {
      return vectorValues.scorer(queryVector);
    }

    final VectorSimilarityFunction vectorSimilarityFunction = fi.getVectorSimilarityFunction();
    return new VectorScorer() {
      final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();

      @Override
      public float score() throws IOException {
        return vectorSimilarityFunction.compare(
            queryVector, vectorValues.vectorValue(iterator.index()));
      }

      @Override
      public DocIdSetIterator iterator() {
        return iterator;
      }
    };
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FloatVectorSimilarityValuesSource other = (FloatVectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "FloatVectorSimilarityValuesSource(fieldName="
        + fieldName
        + " queryVector="
        + Arrays.toString(queryVector)
        + ")";
  }
}
