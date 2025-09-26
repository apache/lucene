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
import java.lang.foreign.MemorySegment;
import java.util.Locale;
import java.util.logging.Logger;
import jdk.incubator.vector.FloatVector;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/** A vectorization provider that leverages the Panama Vector API. */
final class PanamaVectorizationProvider extends VectorizationProvider {

  // NOTE: Avoid static fields or initializers which rely on the vector API, as these initializers
  // would get called before we have a chance to perform sanity checks around the vector API in the
  // constructor of this class. Put them in PanamaVectorConstants instead.

  private final VectorUtilSupport vectorUtilSupport;

  PanamaVectorizationProvider() {
    // hack to work around for JDK-8309727:
    FloatVector.fromArray(
        FloatVector.SPECIES_PREFERRED, new float[FloatVector.SPECIES_PREFERRED.length()], 0);

    if (PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE < 128) {
      throw new UnsupportedOperationException(
          "Vector bit size is less than 128: " + PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE);
    }

    if (PanamaVectorConstants.ENABLE_INTEGER_VECTORS == false) {
      throw new UnsupportedOperationException(
          "CPU type or flags do not guarantee support for fast integer vectorization");
    }

    this.vectorUtilSupport = new PanamaVectorUtilSupport();

    logIncubatorSetup();
  }

  @SuppressForbidden(reason = "We log at info level here, it's fine.")
  private void logIncubatorSetup() {
    var log = Logger.getLogger(getClass().getName());
    log.info(
        String.format(
            Locale.ENGLISH,
            "Java vector incubator API enabled; uses preferredBitSize=%d%s%s",
            PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE,
            Constants.HAS_FAST_VECTOR_FMA ? "; FMA enabled" : "",
            VectorizationProvider.TESTS_VECTOR_SIZE.isPresent() ? "; testMode enabled" : ""));
  }

  @Override
  public VectorUtilSupport getVectorUtilSupport() {
    return vectorUtilSupport;
  }

  @Override
  public FlatVectorsScorer getLucene99FlatVectorsScorer() {
    return Lucene99MemorySegmentFlatVectorsScorer.INSTANCE;
  }

  @Override
  public FlatVectorsScorer getLucene99ScalarQuantizedVectorsScorer() {
    return Lucene99MemorySegmentScalarQuantizedVectorScorer.INSTANCE;
  }

  @Override
  public PostingDecodingUtil newPostingDecodingUtil(IndexInput input) throws IOException {
    if (input instanceof MemorySegmentAccessInput msai) {
      MemorySegment ms = msai.segmentSliceOrNull(0, input.length());
      if (ms != null) {
        return new MemorySegmentPostingDecodingUtil(input, ms);
      }
    }
    return new PostingDecodingUtil(input);
  }
}
