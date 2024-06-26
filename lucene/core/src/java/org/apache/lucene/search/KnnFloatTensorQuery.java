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

import org.apache.lucene.document.KnnFloatTensorField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Syntactic sugar on top of {@link KnnFloatVectorQuery} to provide an interface for tensor query targets.
 */
public class KnnFloatTensorQuery extends KnnFloatVectorQuery {

  // set to 0 if not provided at query creation
  private final int targetDimension;

  /**
   * Find the <code>k</code> nearest documents to the target tensor according to the tensors in the
   * given field. All vector values for the target are concatenated as a
   * single float[] in <code>target</code> tensor.
   *
   * @param field a field that has been indexed as a {@link KnnFloatTensorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @param filter a filter applied before the vector search
   * @param targetDimension dimension for the target tensor. Should be same as that of the indexed field.
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnFloatTensorQuery(String field, float[] target, int k, Query filter, int targetDimension) {
    super(field, target, k, filter);
    this.targetDimension = targetDimension;
  }

  public static KnnFloatTensorQuery create(String field, List<float[]> target, int k, Query filter) {
    if (target == null || target.isEmpty()) {
      throw new IllegalArgumentException("empty target");
    }
    int targetDim = target.get(0).length;
    float[] packedTarget = new float[targetDim * target.size()];
    int dPtr = 0;
    for (float[] v: target) {
      if (v.length != targetDim) {
        throw new IllegalArgumentException("all vectors in the tensor should have the same dimension");
      }
      System.arraycopy(v, 0, packedTarget, dPtr, targetDim);
      dPtr += targetDim;
    }
    return new KnnFloatTensorQuery(field, packedTarget, k, filter, targetDim);
  }

  public static KnnFloatTensorQuery create(String field, float[] target, int k, Query filter) {
    return new KnnFloatTensorQuery(field, target, k, filter, 0);
  }

  public static KnnFloatTensorQuery create(String field, float[] target, int k) {
    return new KnnFloatTensorQuery(field, target, k, null, 0);
  }

  public static KnnFloatTensorQuery create(String field, float[] target, int k, Query filter, int dimension) {
    return new KnnFloatTensorQuery(field, target, k, filter, dimension);
  }

  public static KnnFloatTensorQuery create(String field, float[] target, int k, int dimension) {
    return new KnnFloatTensorQuery(field, target, k, null, dimension);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    int fieldDim = context.reader().getFieldInfos().fieldInfo(field).getTensorDimension();
    if (targetDimension != 0 && fieldDim != targetDimension) {
      throw new IllegalArgumentException("Indexed field " + field +
          " dimension: " + fieldDim + " does not match search target dimension: " + targetDimension);
    }
    return super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    int fieldDim = context.reader().getFieldInfos().fieldInfo(field).getTensorDimension();
    if (targetDimension != 0 && fieldDim != targetDimension) {
      throw new IllegalArgumentException("Indexed field " + field +
          " dimension: " + fieldDim + " does not match search target dimension: " + targetDimension);
    }
    return super.exactSearch(context, acceptIterator, queryTimeout);
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + ":" + this.field + "[" + target[0] + ",...][" + k + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (super.equals(o) == false) return false;
    KnnFloatTensorQuery that = (KnnFloatTensorQuery) o;
    return Arrays.equals(target, that.target);
  }
}
