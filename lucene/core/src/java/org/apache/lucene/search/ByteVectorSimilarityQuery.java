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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.Bits;

/**
 * Search for all (approximate) byte vectors above a similarity score using the {@link
 * VectorSimilarityCollector}.
 *
 * @lucene.experimental
 */
public class ByteVectorSimilarityQuery extends AbstractVectorSimilarityQuery {
  private final byte[] target;

  /**
   * Performs similarity-based byte vector searches using the {@link VectorSimilarityCollector}.
   *
   * @param field a field that has been indexed as a {@link KnnByteVectorField}.
   * @param target the target of the search.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param filter a filter applied before the vector search.
   */
  public ByteVectorSimilarityQuery(
      String field,
      byte[] target,
      float traversalSimilarity,
      float resultSimilarity,
      Query filter) {
    super(field, traversalSimilarity, resultSimilarity, filter);
    this.target = Objects.requireNonNull(target, "target");
  }

  @Override
  @SuppressWarnings("resource")
  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit)
      throws IOException {
    VectorSimilarityCollector vectorSimilarityCollector =
        new VectorSimilarityCollector(traversalSimilarity, resultSimilarity, visitedLimit);
    context.reader().searchNearestVectors(field, target, vectorSimilarityCollector, acceptDocs);
    return vectorSimilarityCollector.topDocs();
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    if (fi.getVectorEncoding() != VectorEncoding.BYTE) {
      return null;
    }
    return VectorScorer.create(context, fi, target);
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + ":"
        + this.field
        + "["
        + target[0]
        + ",...][traversalSimilarity="
        + traversalSimilarity
        + "][resultSimilarity="
        + resultSimilarity
        + "]";
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && Arrays.equals(target, ((ByteVectorSimilarityQuery) o).target);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(target);
    return result;
  }
}
