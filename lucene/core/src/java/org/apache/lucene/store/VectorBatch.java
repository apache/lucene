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
package org.apache.lucene.store;

import java.io.IOException;

/**
 * Accumulates vector reads across several {@link VectorBatchCapable} inputs of the same store and
 * services them with one submission. A KNN rerank shortlist is spread over every segment of an
 * index, so each segment's {@code .vec} input individually sees only a fraction of the query's
 * reads; a store that can issue reads for many files at once (io_uring carries a file descriptor
 * per submission entry) reaches a far deeper queue by taking them together.
 *
 * <p>Add every group, then call {@link #execute()} once; the output arrays are only valid
 * afterwards. A batch belongs to the thread that created it.
 *
 * @lucene.experimental
 */
public interface VectorBatch {

  /**
   * Queues {@code count} vectors of {@code dim} floats from {@code in}, vector {@code i} starting
   * at {@code positions[i]} relative to that input, to be written into {@code out} at {@code i *
   * dim}.
   *
   * @return false if {@code in} is not an input of the same store as this batch, in which case the
   *     caller should read it separately
   */
  boolean add(IndexInput in, long[] positions, int dim, int count, float[] out) throws IOException;

  /** Reads everything queued so far, then clears the batch for reuse. */
  void execute() throws IOException;
}
