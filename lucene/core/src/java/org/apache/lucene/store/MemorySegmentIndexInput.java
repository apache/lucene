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
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import sun.misc.Unsafe;

/**
 * Base IndexInput implementation that uses an array of MemorySegments to represent a file.
 *
 * <p>For efficiency, this class requires that the segment size are a power-of-two (<code>
 * chunkSizePower</code>).
 */
public abstract class MemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
  // We pass 1L as alignment, because currently Lucene file formats are heavy unaligned: :(
  static final VarHandle VH_getByte =
      MemoryHandles.varHandle(byte.class, 1L, ByteOrder.LITTLE_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getShort =
      MemoryHandles.varHandle(short.class, 1L, ByteOrder.LITTLE_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getInt =
      MemoryHandles.varHandle(int.class, 1L, ByteOrder.LITTLE_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getLong =
      MemoryHandles.varHandle(long.class, 1L, ByteOrder.LITTLE_ENDIAN).withInvokeExactBehavior();

  static final boolean IS_LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

  final boolean isClone;
  final long length;
  final long chunkSizeMask;
  final int chunkSizePower;
  final ResourceScope scope;
  final MemorySegment[] segments;

  int curSegmentIndex = -1;
  MemorySegment
      curSegment; // redundant for speed: segments[curSegmentIndex], also marker if closed!
  long curPosition; // relative to curSegment, not globally

  public static MemorySegmentIndexInput newInstance(
      String resourceDescription,
      ResourceScope scope,
      MemorySegment[] segments,
      long length,
      int chunkSizePower) {
    if (segments.length == 1) {
      return new SingleSegmentImpl(
          resourceDescription, scope, segments[0], length, chunkSizePower, false);
    } else {
      return new MultiSegmentImpl(
          resourceDescription, scope, segments, 0, length, chunkSizePower, false);
    }
  }

  private MemorySegmentIndexInput(
      String resourceDescription,
      ResourceScope scope,
      MemorySegment[] segments,
      long length,
      int chunkSizePower,
      boolean isClone) {
    super(resourceDescription);
    this.scope = scope;
    this.segments = segments;
    assert Arrays.stream(segments).map(MemorySegment::scope).allMatch(scope::equals);
    this.length = length;
    this.chunkSizePower = chunkSizePower;
    this.chunkSizeMask = (1L << chunkSizePower) - 1L;
    this.isClone = isClone;
    this.curSegment = segments[0];
  }

  void ensureOpen() {
    if (curSegment == null) {
      throw alreadyClosed();
    }
  }

  RuntimeException wrapAlreadyClosedException(RuntimeException e) {
    if (e instanceof NullPointerException) {
      return alreadyClosed();
    }
    // TODO: maybe open a JDK issue to have a separate, more
    // meaningful exception for unmapped segments:
    if (e.getMessage() != null && e.getMessage().contains("closed")) {
      return alreadyClosed();
    }
    return e;
  }

  RuntimeException handlePositionalIOOBE(String action, long pos) throws IOException {
    if (pos < 0L) {
      return new IllegalArgumentException(action + " negative position: " + this);
    } else {
      throw new EOFException(action + " past EOF: " + this);
    }
  }

  private AlreadyClosedException alreadyClosed() {
    return new AlreadyClosedException("Already closed: " + this);
  }

  @Override
  public final byte readByte() throws IOException {
    try {
      final byte v = (byte) VH_getByte.get(curSegment, curPosition);
      curPosition++;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      do {
        curSegmentIndex++;
        if (curSegmentIndex >= segments.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curSegment = segments[curSegmentIndex];
        curPosition = 0L;
      } while (curSegment.byteSize() == 0L);
      final byte v = (byte) VH_getByte.get(curSegment, curPosition);
      curPosition++;
      return v;
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }
  
  /*
  private static final Unsafe theUnsafe;
  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      theUnsafe = (Unsafe) f.get(null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
  */
  
  private static long copySegmentToHeap(MemorySegment src, long srcOffset, byte[] target, int targetOffset, int len) {
    /*Objects.checkFromIndexSize(srcOffset, len, src.byteSize());
    theUnsafe.copyMemory(null, src.address().toRawLongValue() + srcOffset, 
        target, Unsafe.ARRAY_BYTE_BASE_OFFSET + targetOffset, len);
    */MemorySegment.ofArray(target).asSlice(targetOffset, len).copyFrom(src.asSlice(srcOffset, len));
    return len;
  }

  private static long copySegmentToHeap(MemorySegment src, long srcOffset, long[] target, int targetOffset, int len) {
    final long bytesLen = (long) len << 3;
    /*Objects.checkFromIndexSize(srcOffset, bytesLen, src.byteSize());
    theUnsafe.copyMemory(null, src.address().toRawLongValue() + srcOffset,
        target, Unsafe.ARRAY_LONG_BASE_OFFSET + ((long) targetOffset << 3), bytesLen);
    */MemorySegment.ofArray(target).asSlice((long) targetOffset << 3, bytesLen).copyFrom(src.asSlice(srcOffset, bytesLen));
    return bytesLen;
  }

  private static long copySegmentToHeap(MemorySegment src, long srcOffset, float[] target, int targetOffset, int len) {
    final long bytesLen = (long) len << 2;
    /*Objects.checkFromIndexSize(srcOffset, bytesLen, src.byteSize());
    theUnsafe.copyMemory(null, src.address().toRawLongValue() + srcOffset,
        target, Unsafe.ARRAY_FLOAT_BASE_OFFSET + ((long) targetOffset << 2), bytesLen);
    */MemorySegment.ofArray(target).asSlice((long) targetOffset << 2, bytesLen).copyFrom(src.asSlice(srcOffset, bytesLen));
    return bytesLen;
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    try {
      curPosition += copySegmentToHeap(curSegment, curPosition, b, offset, len);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      readBytesBoundary(b, offset, len);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  private void readBytesBoundary(byte[] b, int offset, int len)
      throws IOException {
    try {
      long curAvail = curSegment.byteSize() - curPosition;
      while (len > curAvail) {
        copySegmentToHeap(curSegment, curPosition, b, offset, (int) curAvail);
        len -= curAvail;
        offset += curAvail;
        curSegmentIndex++;
        if (curSegmentIndex >= segments.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curSegment = segments[curSegmentIndex];
        curPosition = 0L;
        curAvail = curSegment.byteSize();
      }
      curPosition += copySegmentToHeap(curSegment, curPosition, b, offset, len);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    if (IS_LITTLE_ENDIAN) {
      try {
        curPosition += copySegmentToHeap(curSegment, curPosition, dst, offset, length);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException iobe) {
        super.readLongs(dst, offset, length);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    } else {
      super.readLongs(dst, offset, length);
    }
  }

  @Override
  public void readFloats(float[] dst, int offset, int length) throws IOException {
    if (IS_LITTLE_ENDIAN) {
      try {
        curPosition += copySegmentToHeap(curSegment, curPosition, dst, offset, length);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException iobe) {
        super.readFloats(dst, offset, length);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    } else {
      super.readFloats(dst, offset, length);
    }
  }

  @Override
  public final short readShort() throws IOException {
    try {
      final short v = (short) VH_getShort.get(curSegment, curPosition);
      curPosition += Short.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final int readInt() throws IOException {
    try {
      final int v = (int) VH_getInt.get(curSegment, curPosition);
      curPosition += Integer.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final long readLong() throws IOException {
    try {
      final long v = (long) VH_getLong.get(curSegment, curPosition);
      curPosition += Long.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return (((long) curSegmentIndex) << chunkSizePower) + curPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    // we use >> here to preserve negative, so we will catch AIOOBE,
    // in case pos + offset overflows.
    final int si = (int) (pos >> chunkSizePower);
    try {
      if (si != curSegmentIndex) {
        final MemorySegment seg = segments[si];
        // write values, on exception all is unchanged
        this.curSegmentIndex = si;
        this.curSegment = seg;
      }
      this.curPosition = Objects.checkIndex(pos & chunkSizeMask, curSegment.byteSize() + 1);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      throw handlePositionalIOOBE("seek", pos);
    }
  }

  @Override
  public byte readByte(long pos) throws IOException {
    try {
      final int si = (int) (pos >> chunkSizePower);
      return (byte) VH_getByte.get(segments[si], pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      throw handlePositionalIOOBE("read", pos);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  // used only by random access methods to handle reads across boundaries
  private void setPos(long pos, int si) throws IOException {
    try {
      final MemorySegment seg = segments[si];
      // write values, on exception above all is unchanged
      this.curPosition = pos & chunkSizeMask;
      this.curSegmentIndex = si;
      this.curSegment = seg;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      throw handlePositionalIOOBE("read", pos);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public short readShort(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return (short) VH_getShort.get(segments[si], pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public int readInt(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return (int) VH_getInt.get(segments[si], pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public long readLong(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return (long) VH_getLong.get(segments[si], pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public final MemorySegmentIndexInput clone() {
    final MemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
    try {
      clone.seek(getFilePointer());
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }

    return clone;
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is seeked to the beginning.
   */
  @Override
  public final MemorySegmentIndexInput slice(String sliceDescription, long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + ",length="
              + length
              + ",fileLength="
              + this.length
              + ": "
              + this);
    }

    return buildSlice(sliceDescription, offset, length);
  }

  /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */
  MemorySegmentIndexInput buildSlice(String sliceDescription, long offset, long length) {
    ensureOpen();

    final long sliceEnd = offset + length;
    final int startIndex = (int) (offset >>> chunkSizePower);
    final int endIndex = (int) (sliceEnd >>> chunkSizePower);

    // we always allocate one more slice, the last one may be a 0 byte one after truncating with
    // asSlice():
    final MemorySegment slices[] = Arrays.copyOfRange(segments, startIndex, endIndex + 1);

    // set the last segment's limit for the sliced view.
    slices[slices.length - 1] = slices[slices.length - 1].asSlice(0L, sliceEnd & chunkSizeMask);

    offset = offset & chunkSizeMask;

    final String newResourceDescription = getFullSliceDescription(sliceDescription);
    if (slices.length == 1) {
      return new SingleSegmentImpl(
          newResourceDescription,
          scope,
          slices[0].asSlice(offset, length),
          length,
          chunkSizePower,
          true);
    } else {
      return new MultiSegmentImpl(
          newResourceDescription, scope, slices, offset, length, chunkSizePower, true);
    }
  }

  @Override
  public final void close() throws IOException {
    if (curSegment == null) return;

    try {
      curSegment = null;
      if (isClone == false) {
        scope.close();
      }
    } finally {
      // make sure that after close all segments are nulled,
      // so clones can throw AlreadyClosed on NPE:
      Arrays.fill(segments, null);
    }
  }

  /** Optimization of MemorySegmentIndexInput for when there is only one segment. */
  static final class SingleSegmentImpl extends MemorySegmentIndexInput {

    SingleSegmentImpl(
        String resourceDescription,
        ResourceScope scope,
        MemorySegment segment,
        long length,
        int chunkSizePower,
        boolean isClone) {
      super(
          resourceDescription,
          scope,
          new MemorySegment[] {segment},
          length,
          chunkSizePower,
          isClone);
      this.curSegmentIndex = 0;
    }

    @Override
    public void seek(long pos) throws IOException {
      ensureOpen();
      try {
        curPosition = Objects.checkIndex(pos, length + 1);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE("seek", pos);
      }
    }

    @Override
    public long getFilePointer() {
      ensureOpen();
      return curPosition;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      try {
        return (byte) VH_getByte.get(curSegment, pos);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE("read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public short readShort(long pos) throws IOException {
      try {
        return (short) VH_getShort.get(curSegment, pos);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE("read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public int readInt(long pos) throws IOException {
      try {
        return (int) VH_getInt.get(curSegment, pos);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE("read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public long readLong(long pos) throws IOException {
      try {
        return (long) VH_getLong.get(curSegment, pos);
      } catch (
          @SuppressWarnings("unused")
          IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE("read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }
  }

  /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
  static final class MultiSegmentImpl extends MemorySegmentIndexInput {
    private final long offset;

    MultiSegmentImpl(
        String resourceDescription,
        ResourceScope scope,
        MemorySegment[] segments,
        long offset,
        long length,
        int chunkSizePower,
        boolean isClone) {
      super(resourceDescription, scope, segments, length, chunkSizePower, isClone);
      this.offset = offset;
      try {
        seek(0L);
      } catch (IOException ioe) {
        throw new AssertionError(ioe);
      }
      assert curSegment != null && curSegmentIndex >= 0;
    }

    @Override
    public void seek(long pos) throws IOException {
      assert pos >= 0L;
      super.seek(pos + offset);
    }

    @Override
    public long getFilePointer() {
      return super.getFilePointer() - offset;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return super.readByte(pos + offset);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return super.readShort(pos + offset);
    }

    @Override
    public int readInt(long pos) throws IOException {
      return super.readInt(pos + offset);
    }

    @Override
    public long readLong(long pos) throws IOException {
      return super.readLong(pos + offset);
    }

    @Override
    MemorySegmentIndexInput buildSlice(String sliceDescription, long ofs, long length) {
      return super.buildSlice(sliceDescription, this.offset + ofs, length);
    }
  }
}
