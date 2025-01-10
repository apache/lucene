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
package org.apache.lucene.codecs.lucene101;

import java.io.IOException;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.store.IndexInput;

/**
 * Wrapper around an {@link IndexInput} and a {@link ForUtil} that optionally optimizes decoding
 * using vectorization. This class mostly exists to enable benchmarking the decoding logic of
 * postings since it internally calls code that may only be called from the lucene-core JAR.
 */
public final class PostingIndexInput {

  private static final VectorizationProvider VECTORIZATION_PROVIDER =
      VectorizationProvider.getInstance();

  public final ForUtil forUtil;
  public final ForDeltaUtil forDeltaUtil;
  private final PostingDecodingUtil postingDecodingUtil;

  public PostingIndexInput(IndexInput in, ForUtil forUtil, ForDeltaUtil forDeltaUtil)
      throws IOException {
    this.forUtil = forUtil;
    this.forDeltaUtil = forDeltaUtil;
    this.postingDecodingUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(in);
  }

  /** Decode 128 integers stored on {@code bitsPerValues} bits per value into {@code ints}. */
  public void decode(int bitsPerValue, int[] ints) throws IOException {
    forUtil.decode(bitsPerValue, postingDecodingUtil, ints);
  }

  /**
   * Decode 128 integers stored on {@code bitsPerValues} bits per value, compute their prefix sum,
   * and store results into {@code ints}.
   */
  public void decodeAndPrefixSum(int bitsPerValue, int base, int[] ints) throws IOException {
    forDeltaUtil.decodeAndPrefixSum(bitsPerValue, postingDecodingUtil, base, ints);
  }
}
