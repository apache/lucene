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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.Logger;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorShape;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/** A vectorization provider that leverages the Panama Vector API. */
final class PanamaVectorizationProvider extends VectorizationProvider {

  /** Preferred with in bits for vectors. */
  static final int PREFERRED_VECTOR_BITSIZE;

  /** Whether integer vectors can be trusted to actually be fast. */
  static final boolean HAS_FAST_INTEGER_VECTORS;

  private final VectorUtilSupport vectorUtilSupport;

  static {
    // default to platform supported bitsize
    int vectorBitSize = VectorShape.preferredShape().vectorBitSize();
    // but allow easy overriding for testing
    PREFERRED_VECTOR_BITSIZE = VectorizationProvider.TESTS_VECTOR_SIZE.orElse(vectorBitSize);

    // hotspot misses some SSE intrinsics, workaround it
    // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
    boolean isAMD64withoutAVX2 =
        Constants.OS_ARCH.equals("amd64") && PREFERRED_VECTOR_BITSIZE < 256;
    HAS_FAST_INTEGER_VECTORS = TESTS_FORCE_INTEGER_VECTORS || (isAMD64withoutAVX2 == false);
  }

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  PanamaVectorizationProvider() {
    // hack to work around for JDK-8309727:
    try {
      doPrivileged(
          () ->
              FloatVector.fromArray(
                  FloatVector.SPECIES_PREFERRED,
                  new float[FloatVector.SPECIES_PREFERRED.length()],
                  0));
    } catch (SecurityException se) {
      throw new UnsupportedOperationException(
          "We hit initialization failure described in JDK-8309727: " + se);
    }

    if (PanamaVectorUtilSupport.VECTOR_BITSIZE < 128) {
      throw new UnsupportedOperationException(
          "Vector bit size is less than 128: " + PanamaVectorUtilSupport.VECTOR_BITSIZE);
    }

    this.vectorUtilSupport = new PanamaVectorUtilSupport();

    var log = Logger.getLogger(getClass().getName());
    log.info(
        String.format(
            Locale.ENGLISH,
            "Java vector incubator API enabled; uses preferredBitSize=%d%s%s",
            PanamaVectorUtilSupport.VECTOR_BITSIZE,
            Constants.HAS_FAST_VECTOR_FMA ? "; FMA enabled" : "",
            HAS_FAST_INTEGER_VECTORS ? "" : "; floating-point vectors only"));
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
  public PostingDecodingUtil getPostingDecodingUtil(IndexInput input) throws IOException {
    if (input instanceof MemorySegmentAccessInput msai) {
      MemorySegment ms = msai.segmentSliceOrNull(0, input.length());
      if (ms != null) {
        Optional<PostingDecodingUtil> optional = MemorySegmentPostingDecodingUtil.wrap(input, ms);
        if (optional.isPresent()) {
          return optional.get();
        }
      }
    }
    return new DefaultPostingDecodingUtil(input);
  }
}
