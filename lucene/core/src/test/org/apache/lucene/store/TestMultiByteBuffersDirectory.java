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
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.lucene.tests.store.BaseChunkedDirectoryTestCase;
import org.apache.lucene.util.BitUtil;

/** Tests ByteBuffersDirectory's chunking */
public class TestMultiByteBuffersDirectory extends BaseChunkedDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path, int maxChunkSize) throws IOException {
    // round down huge values (above 20) to keep RAM usage low in tests (especially in nightly)
    int bitsPerBlock =
        Math.min(
            20,
            Math.max(
                ByteBuffersDataOutput.LIMIT_MIN_BITS_PER_BLOCK,
                Integer.numberOfTrailingZeros(BitUtil.nextHighestPowerOfTwo(maxChunkSize))));
    Supplier<ByteBuffersDataOutput> outputSupplier =
        () -> {
          return new ByteBuffersDataOutput(
              bitsPerBlock,
              bitsPerBlock,
              ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
              ByteBuffersDataOutput.NO_REUSE);
        };
    return new ByteBuffersDirectory(
        new SingleInstanceLockFactory(),
        outputSupplier,
        ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS);
  }
}
