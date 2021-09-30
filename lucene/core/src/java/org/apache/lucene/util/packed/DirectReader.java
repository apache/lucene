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
package org.apache.lucene.util.packed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongUnaryOperator;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectWriter}
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = 100;
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = DirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i &lt; numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 *
 * @see DirectWriter
 */
public final class DirectReader extends LongValues {

  /**
   * Retrieves an instance from the specified slice written decoding {@code bitsPerValue} for each
   * value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue) {
    return getInstance(slice, bitsPerValue, 0);
  }

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset) {
    return new DirectReader(slice, bitsPerValue, offset);
  }

  private final LongUnaryOperator decoder;
  private final RandomAccessInput in;
  private final long offset;

  private DirectReader(RandomAccessInput in, int bitsPerValue, long offset) {
    this.in = in;
    this.offset = offset;
    // TODO: Replace by new switch statement in Java 17
    switch (bitsPerValue) {
      case 1:
        decoder = this::get1;
        break;
      case 2:
        decoder = this::get2;
        break;
      case 4:
        decoder = this::get4;
        break;
      case 8:
        decoder = this::get8;
        break;
      case 12:
        decoder = this::get12;
        break;
      case 16:
        decoder = this::get16;
        break;
      case 20:
        decoder = this::get20;
        break;
      case 24:
        decoder = this::get24;
        break;
      case 28:
        decoder = this::get28;
        break;
      case 32:
        decoder = this::get32;
        break;
      case 40:
        decoder = this::get40;
        break;
      case 48:
        decoder = this::get48;
        break;
      case 56:
        decoder = this::get56;
        break;
      case 64:
        decoder = this::get64;
        break;
      default:
        throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  @Override
  public long get(long index) {
    return decoder.applyAsLong(index);
  }

  private long get1(long index) {
    try {
      int shift = (int) (index & 7);
      return (in.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get2(long index) {
    try {
      int shift = ((int) (index & 3)) << 1;
      return (in.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get4(long index) {
    try {
      int shift = (int) (index & 1) << 2;
      return (in.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get8(long index) {
    try {
      return in.readByte(offset + index) & 0xFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get12(long index) {
    try {
      long offset = (index * 12) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readShort(this.offset + offset) >>> shift) & 0xFFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get16(long index) {
    try {
      return in.readShort(offset + (index << 1)) & 0xFFFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get20(long index) {
    try {
      long offset = (index * 20) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get24(long index) {
    try {
      return in.readInt(this.offset + index * 3) & 0xFFFFFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get28(long index) {
    try {
      long offset = (index * 28) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFFFF;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get32(long index) {
    try {
      return in.readInt(this.offset + (index << 2)) & 0xFFFFFFFFL;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get40(long index) {
    try {
      return in.readLong(this.offset + index * 5) & 0xFFFFFFFFFFL;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get48(long index) {
    try {
      return in.readLong(this.offset + index * 6) & 0xFFFFFFFFFFFFL;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get56(long index) {
    try {
      return in.readLong(this.offset + index * 7) & 0xFFFFFFFFFFFFFFL;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long get64(long index) {
    try {
      return in.readLong(offset + (index << 3));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
