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
package org.apache.lucene.index;

import org.apache.lucene.util.Counter;

/**
 * A lazily-allocated shared scratch buffer owned by {@link IndexingChain} and provided to per-field
 * writers that need transient buffers during indexing.
 *
 * <p>Because {@link IndexingChain} (and the {@link DocumentsWriterPerThread} it belongs to) indexes
 * documents single-threadedly, a single shared buffer is safe to reuse across all writers within
 * the same chain. Callers must treat the buffer as transient scratch: fill it, drain it within the
 * same call, and not retain a reference across calls.
 */
final class SharedIndexingBuffer {

  /**
   * Size in bytes of the shared points staging buffer. Sized to match {@link
   * org.apache.lucene.util.PagedBytes} block size (blockBits=12 → 4 KB) so that each chunk drains
   * into at most two PagedBytes blocks.
   */
  static final int POINTS_BUFFER_BYTES = 4 * 1024;

  private final Counter bytesUsed;
  private byte[] bytesScratchBuffer;

  SharedIndexingBuffer(Counter bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  /**
   * Returns the shared points staging buffer, allocating it on the first call and tracking its RAM
   * via the {@link Counter} supplied at construction.
   *
   * <p>Callers must treat the returned array as transient scratch.
   */
  byte[] bytesScratch() {
    if (bytesScratchBuffer == null) {
      bytesScratchBuffer = new byte[POINTS_BUFFER_BYTES];
      bytesUsed.addAndGet(POINTS_BUFFER_BYTES);
    }
    return bytesScratchBuffer;
  }
}
