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

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link DataOutput} for appending data to a file in a {@link Directory}.
 *
 * <p>Instances of this class are <b>not</b> thread-safe.
 *
 * @see Directory
 * @see IndexInput
 */
public abstract class IndexOutput extends DataOutput implements Closeable {

  /**
   * Full description of this output, e.g. which class such as {@code FSIndexOutput}, and the full
   * path to the file
   */
  private final String resourceDescription;

  /** Just the name part from {@code resourceDescription} */
  private final String name;

  /**
   * Sole constructor. resourceDescription should be non-null, opaque string describing this
   * resource; it's returned from {@link #toString}.
   */
  protected IndexOutput(String resourceDescription, String name) {
    if (resourceDescription == null) {
      throw new IllegalArgumentException("resourceDescription must not be null");
    }
    this.resourceDescription = resourceDescription;
    this.name = name;
  }

  /**
   * Returns the name used to create this {@code IndexOutput}. This is especially useful when using
   * {@link Directory#createTempOutput}.
   */
  // TODO: can we somehow use this as the default resource description or something?
  public String getName() {
    return name;
  }

  /** Closes this stream to further operations. */
  @Override
  public abstract void close() throws IOException;

  /** Returns the current position in this file, where the next write will occur. */
  public abstract long getFilePointer();

  /** Returns the current checksum of bytes written so far */
  public abstract long getChecksum() throws IOException;

  @Override
  public String toString() {
    return resourceDescription;
  }

  /**
   * Aligns the current file pointer to multiples of 8 bytes to improve reads with mmap. This will
   * write between 0 and 7 zero bytes using {@link #writeByte(byte)}.
   *
   * @return the new file pointer after alignment
   */
  public final long alignFilePointer() throws IOException {
    final long offset = getFilePointer(), alignedOffset = alignOffset(offset);
    final int count = (int) (alignedOffset - offset);
    for (int i = 0; i < count; i++) {
      writeByte((byte) 0);
    }
    return alignedOffset;
  }

  /** Aligns the given offset to multiples of 8 bytes by rounding up. */
  public static final long alignOffset(long v) {
    return (v + 7L) & (-8L);
  }
}
