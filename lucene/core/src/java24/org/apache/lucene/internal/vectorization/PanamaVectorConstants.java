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

import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.Constants;

/** Shared constants for implementations that take advantage of the Panama Vector API. */
final class PanamaVectorConstants {

  /** Preferred width in bits for vectors. */
  static final int PREFERRED_VECTOR_BITSIZE;

  /** Whether integer vectors can be trusted to actually be fast. */
  static final boolean ENABLE_INTEGER_VECTORS;

  static final VectorSpecies<Integer> PRERERRED_INT_SPECIES;
  static final VectorSpecies<Double> PREFERRED_DOUBLE_SPECIES;

  static {
    // default to platform supported bitsize
    int vectorBitSize = VectorShape.preferredShape().vectorBitSize();
    // but allow easy overriding for testing
    PREFERRED_VECTOR_BITSIZE = VectorizationProvider.TESTS_VECTOR_SIZE.orElse(vectorBitSize);

    // hotspot misses some SSE intrinsics, workaround it
    // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
    boolean isAMD64withoutAVX2 =
        Constants.OS_ARCH.equals("amd64") && PREFERRED_VECTOR_BITSIZE < 256;
    ENABLE_INTEGER_VECTORS =
        (isAMD64withoutAVX2 == false) || VectorizationProvider.TESTS_VECTOR_SIZE.isPresent();

    PRERERRED_INT_SPECIES =
        VectorSpecies.of(int.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
    PREFERRED_DOUBLE_SPECIES =
        VectorSpecies.of(double.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
  }

  private PanamaVectorConstants() {}
}
