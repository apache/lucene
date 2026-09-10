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
 * Optional capability of an {@link IndexInput} whose backing store can fetch many fixed-size float
 * vectors at scattered offsets as one batch, potentially in parallel. Implementations backed by a
 * positional, thread-safe source (for example a file opened with direct I/O) can service the batch
 * concurrently, which the KNN full-precision rerank path uses to read a candidate shortlist without
 * serializing on a single cursor. Inputs that do not implement this interface are read serially.
 *
 * @lucene.experimental
 */
public interface ParallelVectorReadable {

  /**
   * Reads {@code count} vectors, where vector {@code i} is {@code dim} little-endian floats
   * starting at {@code positions[i]} relative to the start of this input. Reads may run
   * concurrently and must not disturb this input's own file pointer.
   *
   * @param positions start of each vector, relative to this input, one per vector
   * @param dim number of floats per vector
   * @param count number of vectors to read (may be less than {@code positions.length})
   * @param out receives the vectors back to back: vector {@code i} occupies {@code out[i * dim]} to
   *     {@code out[i * dim + dim - 1]}. Must have length at least {@code count * dim}.
   */
  void readVectors(long[] positions, int dim, int count, float[] out) throws IOException;
}
