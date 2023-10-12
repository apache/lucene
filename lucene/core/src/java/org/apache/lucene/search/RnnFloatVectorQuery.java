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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

/**
 * Search for all (approximate) float vectors within a radius using the {@link RnnCollector}.
 *
 * @lucene.experimental
 */
public class RnnFloatVectorQuery extends AbstractRnnVectorQuery {
  private final float[] target;

  /**
   * Performs radius-based float vector searches using the {@link RnnCollector}.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param traversalThreshold similarity score corresponding to outer radius of graph traversal.
   * @param resultThreshold similarity score corresponding to inner radius of result collection.
   * @param filter a filter applied before the vector search.
   */
  public RnnFloatVectorQuery(
      String field, float[] target, float traversalThreshold, float resultThreshold, Query filter) {
    super(field, traversalThreshold, resultThreshold, filter);
    this.target = VectorUtil.checkFinite(Objects.requireNonNull(target, "target"));
  }

  @Override
  @SuppressWarnings("resource")
  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit)
      throws IOException {
    RnnCollector rnnCollector = new RnnCollector(traversalThreshold, resultThreshold, visitedLimit);
    context.reader().searchNearestVectors(field, target, rnnCollector, acceptDocs);
    return rnnCollector.topDocs();
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    if (fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
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
        + ",...][traversalThreshold="
        + traversalThreshold
        + "][resultThreshold="
        + resultThreshold
        + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RnnFloatVectorQuery that = (RnnFloatVectorQuery) o;
    return Arrays.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(target);
    return result;
  }
}
