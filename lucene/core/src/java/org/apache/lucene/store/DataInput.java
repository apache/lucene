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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.util.BitUtil;

/**
 * Abstract base class for performing read operations of Lucene's low-level data types.
 *
 * <p>{@code DataInput} may only be used from one thread, because it is not thread safe (it keeps
 * internal state like file position). To allow multithreaded use, every {@code DataInput} instance
 * must be cloned before used in another thread. Subclasses must therefore implement {@link
 * #clone()}, returning a new {@code DataInput} which operates on the same underlying resource, but
 * positioned independently.
 */
public abstract class DataInput implements Cloneable {

  /**
   * Reads and returns a single byte.
   *
   * @see DataOutput#writeByte(byte)
   */
  public abstract byte readByte() throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @see DataOutput#writeBytes(byte[],int)
   */
  public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset with control over
   * whether the read should be buffered (callers who have their own buffer should pass in "false"
   * for useBuffer). Currently only {@link BufferedIndexInput} respects this parameter.
   *
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @param useBuffer set to false if the caller will handle buffering.
   * @see DataOutput#writeBytes(byte[],int)
   */
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    // Default to ignoring useBuffer entirely
    readBytes(b, offset, len);
  }

  /**
   * Reads two bytes and returns a short (LE byte order).
   *
   * @see DataOutput#writeShort(short)
   * @see BitUtil#VH_LE_SHORT
   */
  public short readShort() throws IOException {
    final byte b1 = readByte();
    final byte b2 = readByte();
    return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
  }

  /**
   * Reads four bytes and returns an int (LE byte order).
   *
   * @see DataOutput#writeInt(int)
   * @see BitUtil#VH_LE_INT
   */
  public int readInt() throws IOException {
    final byte b1 = readByte();
    final byte b2 = readByte();
    final byte b3 = readByte();
    final byte b4 = readByte();
    return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
  }

  /**
   * Reads an int stored in variable-length format. Reads between one and five bytes. Smaller values
   * take fewer bytes. Negative numbers are supported, but should be avoided.
   *
   * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
   *
   * @see DataOutput#writeVInt(int)
   */
  public int readVInt() throws IOException {
    int i = 0;
    for (int shift = 0; shift < 32; shift += 7) {
      byte b = readByte();
      i |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
    }
    return i;
  }

  /**
   * Read a {@link BitUtil#zigZagDecode(int) zig-zag}-encoded {@link #readVInt() variable-length}
   * integer.
   *
   * @see DataOutput#writeZInt(int)
   */
  public int readZInt() throws IOException {
    return BitUtil.zigZagDecode(readVInt());
  }

  /**
   * Reads eight bytes and returns a long (LE byte order).
   *
   * @see DataOutput#writeLong(long)
   * @see BitUtil#VH_LE_LONG
   */
  public long readLong() throws IOException {
    return (readInt() & 0xFFFFFFFFL) | (((long) readInt()) << 32);
  }

  /**
   * Read a specified number of longs.
   *
   * @lucene.experimental
   */
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, dst.length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = readLong();
    }
  }

  /**
   * Reads a specified number of ints into an array at the specified offset.
   *
   * @param dst the array to read bytes into
   * @param offset the offset in the array to start storing ints
   * @param length the number of ints to read
   */
  public void readInts(int[] dst, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, dst.length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = readInt();
    }
  }

  /**
   * Reads a specified number of floats into an array at the specified offset.
   *
   * @param floats the array to read bytes into
   * @param offset the offset in the array to start storing floats
   * @param len the number of floats to read
   */
  public void readFloats(float[] floats, int offset, int len) throws IOException {
    Objects.checkFromIndexSize(offset, len, floats.length);
    for (int i = 0; i < len; i++) {
      floats[offset + i] = Float.intBitsToFloat(readInt());
    }
  }

  /**
   * Reads a long stored in variable-length format. Reads between one and nine bytes. Smaller values
   * take fewer bytes. Negative numbers are not supported.
   *
   * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
   *
   * @see DataOutput#writeVLong(long)
   */
  public long readVLong() throws IOException {
    long i = 0;
    // NB: we may be called internally to decode negative (10 byte) values.
    for (int shift = 0; shift < 64; shift += 7) {
      byte b = readByte();
      i |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
    }
    return i;
  }

  /**
   * Read a {@link BitUtil#zigZagDecode(long) zig-zag}-encoded {@link #readVLong() variable-length}
   * integer. Reads between one and ten bytes.
   *
   * @see DataOutput#writeZLong(long)
   */
  public long readZLong() throws IOException {
    return BitUtil.zigZagDecode(readVLong());
  }

  /**
   * Reads a string.
   *
   * @see DataOutput#writeString(String)
   */
  public String readString() throws IOException {
    int length = readVInt();
    final byte[] bytes = new byte[length];
    readBytes(bytes, 0, length);
    return new String(bytes, 0, length, StandardCharsets.UTF_8);
  }

  /**
   * Returns a clone of this stream.
   *
   * <p>Clones of a stream access the same data, and are positioned at the same point as the stream
   * they were cloned from.
   *
   * <p>Expert: Subclasses must ensure that clones may be positioned at different points in the
   * input from each other and from the stream they were cloned from.
   */
  @Override
  public DataInput clone() {
    try {
      return (DataInput) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("This cannot happen: Failing to clone DataInput", e);
    }
  }

  /**
   * Reads a Map&lt;String,String&gt; previously written with {@link
   * DataOutput#writeMapOfStrings(Map)}.
   *
   * @return An immutable map containing the written contents.
   */
  public Map<String, String> readMapOfStrings() throws IOException {
    int count = readVInt();
    if (count == 0) {
      return Map.of();
    } else if (count == 1) {
      return Map.of(readString(), readString());
    } else {
      @SuppressWarnings("unchecked")
      Map.Entry<String, String>[] entries =
          (Map.Entry<String, String>[]) new Map.Entry<?, ?>[count];
      for (int i = 0; i < count; i++) {
        entries[i] = Map.entry(readString(), readString());
      }
      return Map.ofEntries(entries);
    }
  }

  /**
   * Reads a Set&lt;String&gt; previously written with {@link DataOutput#writeSetOfStrings(Set)}.
   *
   * @return An immutable set containing the written contents.
   */
  public Set<String> readSetOfStrings() throws IOException {
    int count = readVInt();
    if (count == 0) {
      return Set.of();
    } else if (count == 1) {
      return Set.of(readString());
    } else {
      String[] set = new String[count];
      for (int i = 0; i < count; i++) {
        set[i] = readString();
      }
      return Set.of(set);
    }
  }

  /**
   * Skip over <code>numBytes</code> bytes. This method may skip bytes in whatever way is most
   * optimal, and may not have the same behavior as reading the skipped bytes. In general, negative
   * <code>numBytes</code> are not supported.
   */
  public abstract void skipBytes(final long numBytes) throws IOException;
}
