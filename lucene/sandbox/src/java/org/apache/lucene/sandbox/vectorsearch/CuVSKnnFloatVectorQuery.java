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
package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

/** Query for CuVS */
public class CuVSKnnFloatVectorQuery extends KnnFloatVectorQuery {

  private final int iTopK;
  private final int searchWidth;

  public CuVSKnnFloatVectorQuery(String field, float[] target, int k, int iTopK, int searchWidth) {
    super(field, target, k);
    this.iTopK = iTopK;
    this.searchWidth = searchWidth;
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {

    PerLeafCuVSKnnCollector results = new PerLeafCuVSKnnCollector(k, iTopK, searchWidth);

    LeafReader reader = context.reader();
    reader.searchNearestVectors(field, this.getTargetCopy(), results, null);
    return results.topDocs();
  }
}
