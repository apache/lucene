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

import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.util.BitUtil; // javadocs

/**
 * Random Access Index API. Unlike {@link IndexInput}, this has no concept of file position, all
 * reads are absolute. However, like {@link IndexInput}, it is only intended for use by a single
 * thread.
 *
 * <p><b>Warning:</b> Casting an {@link IndexInput} directly to {@code RandomAccessInput} is not
 * supported, although implementations may implement this interface. The only supported way to
 * obtain a {@code RandomAccessInput} from an {@link IndexInput} is by calling {@link
 * IndexInput#randomAccessSlice(long, long)}, which always returns a <b>new</b> slice, independent
 * from the original input. Casting the original input may modify the state of the {@link
 * IndexInput} when doing random accesses and needs to be avoided.
 *
 * <p>If any read goes beyond the end of the input, it will throw {@link EOFException}. If any read
 * throws an {@link IOException} (including {@link EOFException}), then the input may be left in an
 * undetermined state.
 */
public interface RandomAccessInput {

  /** The number of bytes in the file. */
  long length();

  /**
   * Reads a byte at the given position in the file
   *
   * @see DataInput#readByte
   */
  byte readByte(long pos) throws IOException;

  /**
   * Reads a specified number of bytes starting at a given position into an array at the specified
   * offset.
   *
   * @see DataInput#readBytes
   */
  default void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      bytes[offset + i] = readByte(pos + i);
    }
  }

  /**
   * Reads a short (LE byte order) at the given position in the file
   *
   * @see DataInput#readShort
   * @see BitUtil#VH_LE_SHORT
   */
  short readShort(long pos) throws IOException;

  /**
   * Reads an integer (LE byte order) at the given position in the file
   *
   * @see DataInput#readInt
   * @see BitUtil#VH_LE_INT
   */
  int readInt(long pos) throws IOException;

  /**
   * Reads a long (LE byte order) at the given position in the file
   *
   * @see DataInput#readLong
   * @see BitUtil#VH_LE_LONG
   */
  long readLong(long pos) throws IOException;

  /**
   * Prefetch data in the background.
   *
   * @see IndexInput#prefetch
   */
  default boolean prefetch(long offset, long length) throws IOException {
    return false;
  }

  /**
   * Returns a hint whether all the contents of this input are resident in physical memory.
   *
   * @see IndexInput#isLoaded()
   */
  default Optional<Boolean> isLoaded() {
    return Optional.empty();
  }
}
