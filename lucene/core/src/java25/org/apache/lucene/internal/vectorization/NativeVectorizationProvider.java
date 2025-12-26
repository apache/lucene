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

package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;

/** Native provider returning native implemented implementations. */
final class NativeVectorizationProvider extends VectorizationProvider {

  private final VectorizationProvider delegateVectorUtilProvider =
      new DefaultVectorizationProvider();

  private final VectorUtilSupport vectorUtilSupport;

  NativeVectorizationProvider() {
    vectorUtilSupport =
        new NativeVectorUtilSupport(delegateVectorUtilProvider.getVectorUtilSupport());
  }

  @Override
  public VectorUtilSupport getVectorUtilSupport() {
    return vectorUtilSupport;
  }

  @Override
  public FlatVectorsScorer getLucene99FlatVectorsScorer() {
    return delegateVectorUtilProvider.getLucene99FlatVectorsScorer();
  }

  @Override
  public FlatVectorsScorer getLucene99ScalarQuantizedVectorsScorer() {
    return delegateVectorUtilProvider.getLucene99ScalarQuantizedVectorsScorer();
  }

  @Override
  public PostingDecodingUtil newPostingDecodingUtil(IndexInput input) throws IOException {
    return delegateVectorUtilProvider.newPostingDecodingUtil(input);
  }
}
