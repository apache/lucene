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
import org.apache.lucene.index.LeafReaderContext;

/**
 * A {@link DoubleValuesSource} which computes the vector similarity scores between the query vector
 * and the {@link org.apache.lucene.document.KnnFloatVectorField} for documents.
 */
class FloatVectorSimilarityValuesSource extends VectorSimilarityValuesSource {

  private final float[] queryVector;

  public FloatVectorSimilarityValuesSource(float[] vector, String fieldName) {
    super(fieldName);
    this.queryVector = vector;
  }

  @Override
  public VectorScorer getScorer(LeafReaderContext ctx) throws IOException {
    final FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
    if (vectorValues == null) {
      FloatVectorValues.checkField(ctx.reader(), fieldName);
      return null;
    }
    return vectorValues.scorer(queryVector);
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
