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
import java.lang.foreign.MemorySegment;

/**
 * Provides access to the backing memory segment.
 *
 * <p>Expert API, allows access to the backing memory.
 */
public interface MemorySegmentAccessInput extends RandomAccessInput, Cloneable {

  /** Returns the memory segment for a given position and length, or null. */
  MemorySegment segmentSliceOrNull(long pos, int len) throws IOException;

  default void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      bytes[offset + i] = readByte(pos + i);
    }
  }

  MemorySegmentAccessInput clone();
}
