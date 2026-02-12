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
import java.util.Locale;
import java.util.logging.Logger;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/** Native provider returning native implementations which delegates to Panama implementation. */
public final class NativeVectorizationProvider extends VectorizationProvider {

  private final VectorizationProvider delegateVectorUtilProvider =
      new PanamaVectorizationProvider();

  private final VectorUtilSupport vectorUtilSupport;

  NativeVectorizationProvider() {
    vectorUtilSupport =
        new NativeVectorUtilSupport(delegateVectorUtilProvider.getVectorUtilSupport());
    logIncubatorSetup();
  }

  @SuppressForbidden(reason = "We log at info level here, it's fine.")
  private void logIncubatorSetup() {
    var log = Logger.getLogger(getClass().getName());
    log.info(
        String.format(
            Locale.ENGLISH,
            "Native vectorization enabled; uses preferredBitSize=%d%s%s; strict mode=%s; native library loaded=%s;",
            PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE,
            Constants.HAS_FAST_VECTOR_FMA ? "; FMA enabled" : "",
            VectorizationProvider.TESTS_VECTOR_SIZE.isPresent() ? "; testMode enabled" : "",
            Constants.NATIVE_STRICT_MODE ? "true" : "false",
            NativeVectorUtilSupport.isLibraryLoaded()
                ? "true, using " + getClass().getSimpleName()
                : "false, switching to "
                    + delegateVectorUtilProvider.getClass().getSimpleName()
                    + "as fallback"));
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
