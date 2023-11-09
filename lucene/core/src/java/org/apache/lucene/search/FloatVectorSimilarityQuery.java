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
import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.VectorUtil;

/**
 * Search for all (approximate) float vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
public class FloatVectorSimilarityQuery extends AbstractVectorSimilarityQuery {
  private final float[] target;

  /**
   * Search for all (approximate) float vectors above a similarity threshold. First performs a
   * similarity-based graph search using {@link VectorSimilarityCollector} between {@link
   * #traversalSimilarity} and {@link #resultSimilarity}. If this does not complete within a
   * specified {@link #visitLimit}, returns a lazy-loading iterator over all float vectors above the
   * {@link #resultSimilarity}.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param visitLimit limit on number of nodes to visit before falling back to a lazy-loading
   *     iterator.
   */
  public FloatVectorSimilarityQuery(
      String field,
      float[] target,
      float traversalSimilarity,
      float resultSimilarity,
      long visitLimit) {
    super(field, traversalSimilarity, resultSimilarity, visitLimit);
    this.target = VectorUtil.checkFinite(Objects.requireNonNull(target, "target"));
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context) throws IOException {
    @SuppressWarnings("resource")
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
    return VectorScorer.create(context, fi, target);
  }

  @Override
  protected void approximateSearch(LeafReaderContext context, KnnCollector collector)
      throws IOException {
    @SuppressWarnings("resource")
    LeafReader reader = context.reader();
    reader.searchNearestVectors(field, target, collector, reader.getLiveDocs());
  }

  @Override
  public String toString(String field) {
    return String.format(
        Locale.ROOT,
        "%s[field=%s target=[%f...] traversalSimilarity=%f resultSimilarity=%f visitLimit=%d]",
        getClass().getSimpleName(),
        field,
        target[0],
        traversalSimilarity,
        resultSimilarity,
        visitLimit);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o)
        && super.equals(o)
        && Arrays.equals(target, ((FloatVectorSimilarityQuery) o).target);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(target);
    return result;
  }
}
