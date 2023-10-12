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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;

/**
 * An abstract class that provides the vector similarity scores between the query vector and the
 * {@link org.apache.lucene.document.KnnFloatVectorField} or {@link
 * org.apache.lucene.document.KnnByteVectorField} for documents.
 */
abstract class VectorSimilarityValuesSource extends DoubleValuesSource {
  protected final String fieldName;

  public VectorSimilarityValuesSource(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Returns a DoubleValues instance for computing the vector similarity score per document against
   * the byte query vector
   *
   * @param ctx the context for which to return the DoubleValues
   * @param queryVector byte query vector
   * @param vectorField knn byte field name
   * @return DoubleValues instance
   * @throws IOException if an {@link IOException} occurs
   */
  public static DoubleValues similarityToQueryVector(
      LeafReaderContext ctx, byte[] queryVector, String vectorField) throws IOException {
    if (ctx.reader().getFieldInfos().fieldInfo(vectorField).getVectorEncoding()
        != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "Field "
              + vectorField
              + " does not have the expected vector encoding: "
              + VectorEncoding.BYTE);
    }
    return new ByteVectorSimilarityValuesSource(queryVector, vectorField).getValues(ctx, null);
  }

  /**
   * Returns a DoubleValues instance for computing the vector similarity score per document against
   * the float query vector
   *
   * @param ctx the context for which to return the DoubleValues
   * @param queryVector float query vector
   * @param vectorField knn float field name
   * @return DoubleValues instance
   * @throws IOException if an {@link IOException} occurs
   */
  public static DoubleValues similarityToQueryVector(
      LeafReaderContext ctx, float[] queryVector, String vectorField) throws IOException {
    if (ctx.reader().getFieldInfos().fieldInfo(vectorField).getVectorEncoding()
        != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Field "
              + vectorField
              + " does not have the expected vector encoding: "
              + VectorEncoding.FLOAT32);
    }
    return new FloatVectorSimilarityValuesSource(queryVector, vectorField).getValues(ctx, null);
  }

  @Override
  public abstract DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
      throws IOException;

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
