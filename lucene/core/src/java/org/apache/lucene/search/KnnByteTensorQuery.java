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

import org.apache.lucene.document.KnnByteTensorField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Syntactic sugar on top of {@link KnnByteVectorQuery} to provide an interface for tensor query targets.
 */
public class KnnByteTensorQuery extends KnnByteVectorQuery {

  // set to 0 if not provided at query creation
  private final int targetDimension;

  /**
   * Find the <code>k</code> nearest documents to the target tensor according to the tensors in the
   * given field. All vector values for the target are concatenated as a
   * single byte[] in <code>target</code> tensor.
   *
   * @param field a field that has been indexed as a {@link KnnByteTensorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @param filter a filter applied before the vector search
   * @param targetDimension dimension for the target tensor. Should be same as that of the indexed field.
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnByteTensorQuery(String field, byte[] target, int k, Query filter, int targetDimension) {
    super(field, target, k, filter);
    this.targetDimension = targetDimension;
  }

  public static KnnByteTensorQuery create(String field, List<byte[]> target, int k, Query filter) {
    if (target == null || target.isEmpty()) {
      throw new IllegalArgumentException("empty target");
    }
    int targetDim = target.get(0).length;
    byte[] packedTarget = new byte[targetDim * target.size()];
    int dPtr = 0;
    for (byte[] v: target) {
      if (v.length != targetDim) {
        throw new IllegalArgumentException("all vectors in the tensor should have the same dimension");
      }
      System.arraycopy(v, 0, packedTarget, dPtr, targetDim);
      dPtr += targetDim;
    }
    return new KnnByteTensorQuery(field, packedTarget, k, filter, targetDim);
  }

  public static KnnByteTensorQuery create(String field, byte[] target, int k, Query filter) {
    return new KnnByteTensorQuery(field, target, k, filter, 0);
  }

  public static KnnByteTensorQuery create(String field, byte[] target, int k) {
    return new KnnByteTensorQuery(field, target, k, null, 0);
  }

  public static KnnByteTensorQuery create(String field, byte[] target, int k, Query filter, int dimension) {
    return new KnnByteTensorQuery(field, target, k, filter, dimension);
  }

  public static KnnByteTensorQuery create(String field, byte[] target, int k, int dimension) {
    return new KnnByteTensorQuery(field, target, k, null, dimension);
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
    KnnByteTensorQuery that = (KnnByteTensorQuery) o;
    return Arrays.equals(target, that.target);
  }
}
