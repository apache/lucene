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
package org.apache.lucene.misc.store;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * A {@link Directory} implementation for all Unixes and Windows that uses DIRECT I/O to bypass OS
 * level IO caching during merging. For all other cases (searching, writing) we delegate to the
 * provided Directory instance.
 *
 * <p>See <a href="{@docRoot}/overview-summary.html#DirectIODirectory">Overview</a> for more
 * details.
 *
 * <p><b>WARNING</b>: this code is very new and quite easily could contain horrible bugs.
 *
 * <p>This directory passes Solr and Lucene tests on Linux, OS X, and Windows; other systems should
 * work but have not been tested! Use at your own risk.
 *
 * <p>@throws UnsupportedOperationException if the operating system, file system or JDK does not
 * support Direct I/O or a sufficient equivalent.
 *
 * @lucene.experimental
 */
public class DirectIODirectory extends FilterDirectory {

  /**
   * Default buffer size before writing to disk (256 KB); larger means less IO load but more RAM and
   * direct buffer storage space consumed during merging.
   */
  public static final int DEFAULT_MERGE_BUFFER_SIZE = 256 * 1024;

  /** Default min expected merge size before direct IO is used (10 MB): */
  public static final long DEFAULT_MIN_BYTES_DIRECT = 10 * 1024 * 1024;

  private final int blockSize, mergeBufferSize;
  private final long minBytesDirect;

  volatile boolean isOpen = true;

  /**
   * Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum
   * lookup. There are two reasons for using this instead of directly referencing
   * ExtendedOpenOption.DIRECT:
   *
   * <ol>
   *   <li>ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes
   *       un-suppressible(?) warning to be emitted when compiling with --release flag and value N,
   *       where N is smaller than the version of javac used for compilation. For details, please
   *       refer to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8259039.
   *   <li>It is possible that Lucene is run using JDK that does not support
   *       ExtendedOpenOption.DIRECT. In such a case, dynamic lookup allows us to bail out with
   *       UnsupportedOperationException with meaningful error message.
   * </ol>
   *
   * <p>This reference is {@code null}, if the JDK does not support direct I/O.
   */
  static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

  static {
    OpenOption option;
    try {
      final Class<? extends OpenOption> clazz =
          Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
      option =
          Arrays.stream(clazz.getEnumConstants())
              .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
              .findFirst()
              .orElse(null);
    } catch (Exception _) {
      option = null;
    }
    ExtendedOpenOption_DIRECT = option;
  }

  /**
   * Create a new DirectIODirectory for the named location.
   *
   * @param delegate Directory for non-merges, also used as reference to file system path.
   * @param mergeBufferSize Size of buffer to use for merging.
   * @param minBytesDirect Merges, or files to be opened for reading, smaller than this will not use
   *     direct IO. See {@link #DEFAULT_MIN_BYTES_DIRECT} and {@link #useDirectIO}.
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect)
      throws IOException {
    super(delegate);
    this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
    this.mergeBufferSize = mergeBufferSize;
    this.minBytesDirect = minBytesDirect;
  }

  /**
   * Create a new DirectIODirectory for the named location.
   *
   * @param delegate Directory for non-merges, also used as reference to file system path.
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(FSDirectory delegate) throws IOException {
    this(delegate, DEFAULT_MERGE_BUFFER_SIZE, DEFAULT_MIN_BYTES_DIRECT);
  }

  /**
   * @return the underlying file system directory
   */
  public Path getDirectory() {
    return ((FSDirectory) in).getDirectory();
  }

  @Override
  protected void ensureOpen() throws AlreadyClosedException {
    if (!isOpen) {
      throw new AlreadyClosedException("this Directory is closed");
    }
  }

  /**
   * Determines if direct IO should be used for a file. By default this tests if it is a merge
   * context and if the merge or file length exceeds the minimum size (see {@link
   * #DEFAULT_MIN_BYTES_DIRECT}). Subclasses may override method to enforce direct IO for specific
   * file types.
   *
   * @param name file name (unused by default implementation)
   * @param context information about merge size
   * @param fileLength if available, gives the file length. Will be empty when requesting an {@link
   *     IndexOutput}.
   * @return {@code true} if direct IO should be used; {@code false} if input/output should be
   *     requested from delegate directory.
   */
  protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
    return context.context() == Context.MERGE
        && context.mergeInfo().estimatedMergeBytes() >= minBytesDirect
        && fileLength.orElse(minBytesDirect) >= minBytesDirect;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (useDirectIO(name, context, OptionalLong.of(fileLength(name)))) {
      return new DirectIOIndexInput(getDirectory().resolve(name), blockSize, mergeBufferSize, 16);
    } else {
      return in.openInput(name, context);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (useDirectIO(name, context, OptionalLong.empty())) {
      return new DirectIOIndexOutput(
          getDirectory().resolve(name), name, blockSize, mergeBufferSize);
    } else {
      return in.createOutput(name, context);
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    super.close();
  }

  private static OpenOption getDirectOpenOption() {
    if (ExtendedOpenOption_DIRECT == null) {
      throw new UnsupportedOperationException(
          "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
    }
    return ExtendedOpenOption_DIRECT;
  }

  private static final class DirectIOIndexOutput extends IndexOutput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final Checksum digest;

    private long filePos;
    private boolean isOpen;

    /**
     * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO
     * bypassing OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support Direct I/O or a
     *     sufficient equivalent.
     */
    public DirectIOIndexOutput(Path path, String name, int blockSize, int bufferSize)
        throws IOException {
      super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

      channel =
          FileChannel.open(
              path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
      buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);
      digest = new BufferedChecksum(new CRC32());

      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      buffer.put(b);
      digest.update(b);
      if (!buffer.hasRemaining()) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while (true) {
        final int left = buffer.remaining();
        if (left <= toWrite) {
          buffer.put(src, offset, left);
          digest.update(src, offset, left);
          toWrite -= left;
          offset += left;
          dump();
        } else {
          buffer.put(src, offset, toWrite);
          digest.update(src, offset, toWrite);
          break;
        }
      }
    }

    private void dump() throws IOException {
      final int size = buffer.position();

      // we need to rewind, as we have to write full blocks (we truncate file later):
      buffer.rewind();

      channel.write(buffer, filePos);
      filePos += size;

      buffer.clear();
    }

    @Override
    public long getFilePointer() {
      return filePos + buffer.position();
    }

    @Override
    public long getChecksum() {
      return digest.getValue();
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        try {
          dump();
        } finally {
          try (FileChannel ch = channel) {
            ch.truncate(getFilePointer());
          }
        }
      }
    }
  }

  private static final class DirectIOIndexInput extends IndexInput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final int blockSize;
    private final long offset;
    private final long length;
    private final boolean isClosable; // clones and slices are not closable
    private boolean isOpen;
    private final DirectIOPrefetcher prefetcher;
    private long filePos;

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support support Direct I/O
     *     or a sufficient equivalent.
     */
    public DirectIOIndexInput(Path path, int blockSize, int bufferSize, int maxPrefetches)
        throws IOException {
      super("DirectIOIndexInput(path=\"" + path + "\")");
      this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
      this.blockSize = blockSize;
      this.buffer = allocateBuffer(bufferSize, blockSize);
      this.prefetcher =
          new DirectIOPrefetcher(bufferSize, blockSize, channel, maxPrefetches, maxPrefetches * 32);
      this.isOpen = true;
      this.isClosable = true;
      this.length = channel.size();
      this.offset = 0L;
      this.filePos = -bufferSize;
      this.buffer.limit(0);
    }

    // for clone/slice
    private DirectIOIndexInput(
        String description, DirectIOIndexInput other, long offset, long length) throws IOException {
      super(description);
      Objects.checkFromIndexSize(offset, length, other.channel.size());
      final int bufferSize = other.buffer.capacity();
      this.buffer = allocateBuffer(bufferSize, other.blockSize);
      this.blockSize = other.blockSize;
      this.channel = other.channel;
      this.prefetcher =
          new DirectIOPrefetcher(
              bufferSize,
              blockSize,
              channel,
              other.prefetcher.maxConcurrentPrefetches,
              other.prefetcher.maxTotalPrefetches);
      this.isOpen = true;
      this.isClosable = false;
      this.length = length;
      this.offset = offset;
      this.filePos = -bufferSize;
      buffer.limit(0);
    }

    static ByteBuffer allocateBuffer(int bufferSize, int blockSize) {
      return ByteBuffer.allocateDirect(bufferSize + blockSize - 1)
          .alignedSlice(blockSize)
          .order(LITTLE_ENDIAN);
    }

    /**
     * Prefetches the given range of bytes. The range will be aligned to blockSize and will be
     * chopped up into chunks of buffer size.
     *
     * @param pos the position to prefetch from, must be non-negative and within file length
     * @param length the length to prefetch, must be non-negative. This length may cause multiple
     *     prefetches to be issued, depending on the buffer size.
     */
    @Override
    public void prefetch(long pos, long length) throws IOException {
      if (pos < 0 || length < 0 || pos + length > this.length) {
        throw new IllegalArgumentException(
            "Invalid prefetch range: pos="
                + pos
                + ", length="
                + length
                + ", fileLength="
                + this.length);
      }
      // check if our current buffer already contains the requested range
      final long absPos = pos + offset;
      final long alignedPos = absPos - (absPos % blockSize);
      // we only prefetch into a single buffer, even if length exceeds buffer size
      // maybe we should improve this...
      prefetcher.prefetch(alignedPos, length);
    }

    @Override
    public void close() throws IOException {
      prefetcher.close();
      if (isOpen && isClosable) {
        channel.close();
        isOpen = false;
      }
    }

    @Override
    public long getFilePointer() {
      long filePointer = filePos + buffer.position() - offset;

      // opening the input and immediately calling getFilePointer without calling readX (and thus
      // refill) first,
      // will result in negative value equal to bufferSize being returned,
      // due to the initialization method filePos = -bufferSize used in constructor.
      assert filePointer == -buffer.capacity() - offset || filePointer >= 0
          : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
      return Math.max(filePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        final long absolutePos = pos + offset;
        if (absolutePos >= filePos && absolutePos <= filePos + buffer.limit()) {
          // the new position is within the existing buffer
          buffer.position(Math.toIntExact(absolutePos - filePos));
        } else {
          seekInternal(pos); // do an actual seek/read
        }
      }
      assert pos == getFilePointer();
    }

    private void seekInternal(long pos) throws IOException {
      final long absPos = pos + offset;
      final long alignedPos = absPos - (absPos % blockSize);
      filePos = alignedPos - buffer.capacity();

      final int delta = (int) (absPos - alignedPos);
      refill(delta, delta);
    }

    private void refill(int bytesToRead) throws IOException {
      assert filePos % blockSize == 0;
      refill(bytesToRead, 0);
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public byte readByte() throws IOException {
      if (!buffer.hasRemaining()) {
        refill(1);
      }

      return buffer.get();
    }

    @Override
    public short readShort() throws IOException {
      if (buffer.remaining() >= Short.BYTES) {
        return buffer.getShort();
      } else {
        return super.readShort();
      }
    }

    @Override
    public int readInt() throws IOException {
      if (buffer.remaining() >= Integer.BYTES) {
        return buffer.getInt();
      } else {
        return super.readInt();
      }
    }

    @Override
    public long readLong() throws IOException {
      if (buffer.remaining() >= Long.BYTES) {
        return buffer.getLong();
      } else {
        return super.readLong();
      }
    }

    private void refill(int bytesToRead, int delta) throws IOException {
      long nextFilePos = filePos + buffer.capacity();

      // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
      // hence throwing EOFException early to maintain buffer state (position in particular)
      if (nextFilePos > offset + length || ((offset + length) - nextFilePos < bytesToRead)) {
        filePos = nextFilePos;
        throw new EOFException("read past EOF: " + this);
      }

      buffer.clear();
      try {
        if (prefetcher.readBytes(nextFilePos, buffer, delta)) {
          // handle potentially differently aligned prefetch buffer
          // this gets tricky as the prefetch buffer is always blockSize aligned
          // but the prefetches might be aligned on an earlier block boundary
          // so we need to adjust the filePos accordingly
          long currentLogicalPos = nextFilePos + delta;
          filePos = currentLogicalPos - buffer.position();
          return;
        }
        filePos = nextFilePos;
        // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
        // EOF
        // when filePos > channel.size(), an EOFException will be thrown from above
        channel.read(buffer, filePos);
        buffer.flip();
        buffer.position(delta);
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      int toRead = len;
      while (true) {
        final int left = buffer.remaining();
        if (left < toRead) {
          buffer.get(dst, offset, left);
          toRead -= left;
          offset += left;
          refill(toRead);
        } else {
          buffer.get(dst, offset, toRead);
          break;
        }
      }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
      int remainingDst = len;
      while (remainingDst > 0) {
        int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
        buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
        buffer.position(buffer.position() + Integer.BYTES * cnt);
        remainingDst -= cnt;
        if (remainingDst > 0) {
          if (buffer.hasRemaining()) {
            dst[offset + len - remainingDst] = readInt();
            --remainingDst;
          } else {
            refill(remainingDst * Integer.BYTES);
          }
        }
      }
    }

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
      int remainingDst = len;
      while (remainingDst > 0) {
        int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
        buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
        buffer.position(buffer.position() + Float.BYTES * cnt);
        remainingDst -= cnt;
        if (remainingDst > 0) {
          if (buffer.hasRemaining()) {
            dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
            --remainingDst;
          } else {
            refill(remainingDst * Float.BYTES);
          }
        }
      }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
      int remainingDst = len;
      while (remainingDst > 0) {
        int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
        buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
        buffer.position(buffer.position() + Long.BYTES * cnt);
        remainingDst -= cnt;
        if (remainingDst > 0) {
          if (buffer.hasRemaining()) {
            dst[offset + len - remainingDst] = readLong();
            --remainingDst;
          } else {
            refill(remainingDst * Long.BYTES);
          }
        }
      }
    }

    @Override
    public DirectIOIndexInput clone() {
      try {
        var clone = new DirectIOIndexInput("clone:" + this, this, offset, length);
        // TODO figure out how to make this async
        clone.seekInternal(getFilePointer());
        return clone;
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      if ((length | offset) < 0 || length > this.length - offset) {
        throw new IllegalArgumentException(
            "slice() " + sliceDescription + " out of bounds: " + this);
      }
      var slice = new DirectIOIndexInput(sliceDescription, this, this.offset + offset, length);
      // TODO figure out how to make this async
      slice.seekInternal(0L);
      return slice;
    }
  }

  /** A prefetcher that can prefetch multiple chunks of data from a FileChannel using direct IO. */
  private static class DirectIOPrefetcher implements Closeable {
    private final int maxConcurrentPrefetches;
    private final int maxTotalPrefetches;
    private final int blockSize;
    private final long[] prefetchPos;
    private final Future<?>[] prefetchThreads;
    private final TreeMap<Long, Integer> posToSlot;
    private final Deque<Integer> slots;
    private final ByteBuffer[] prefetchBuffers;
    private final IOException[] prefetchExceptions;
    private final int prefetchBytesSize;
    private final Deque<Long> pendingPrefetches = new ArrayDeque<>();
    private final FileChannel channel;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    DirectIOPrefetcher(
        int prefetchBytesSize,
        int blockSize,
        FileChannel channel,
        int maxConcurrentPrefetches,
        int maxTotalPrefetches) {
      this.blockSize = blockSize;
      this.maxConcurrentPrefetches = maxConcurrentPrefetches;
      this.prefetchPos = new long[maxConcurrentPrefetches];
      this.prefetchThreads = new Future<?>[maxConcurrentPrefetches];
      this.posToSlot = new TreeMap<>();
      this.slots = new ArrayDeque<>(maxConcurrentPrefetches);
      for (int i = 0; i < maxConcurrentPrefetches; i++) {
        slots.addLast(i);
      }
      this.prefetchExceptions = new IOException[maxConcurrentPrefetches];
      this.prefetchBuffers = new ByteBuffer[maxConcurrentPrefetches];
      this.prefetchBytesSize = prefetchBytesSize;
      this.maxTotalPrefetches = maxTotalPrefetches;
      this.channel = channel;
    }

    /**
     * Initiate prefetch of the given range. The range will be aligned to blockSize and chopped up
     * into chunks of prefetchBytesSize.
     *
     * @param pos the position to prefetch from, must be non-negative and within file length
     * @param length the length to prefetch, must be non-negative.
     */
    void prefetch(long pos, long length) {
      // first determine how many slots we need given the length
      int numSlots =
          (int)
              Math.min((length + prefetchBytesSize - 1) / prefetchBytesSize, Integer.MAX_VALUE - 1);
      while (numSlots > 0
          && (this.posToSlot.size() + this.pendingPrefetches.size()) < maxTotalPrefetches) {
        final int slot;
        Integer existingSlot = this.posToSlot.get(pos);
        if (existingSlot != null && prefetchThreads[existingSlot] != null) {
          // already being prefetched and hasn't been consumed.
          // return early
          return;
        }
        if (this.posToSlot.size() < maxConcurrentPrefetches && slots.isEmpty() == false) {
          slot = slots.removeFirst();
          posToSlot.put(pos, slot);
          prefetchPos[slot] = pos;
        } else {
          slot = -1;
          pendingPrefetches.addLast(pos);
        }
        if (slot != -1) {
          startPrefetch(pos, slot);
        }
        pos += prefetchBytesSize;
        numSlots--;
      }
    }

    /**
     * Try to read the requested bytes from an already prefetched buffer. If the requested bytes are
     * not in a prefetched buffer, return false.
     *
     * @param pos the absolute position to read from
     * @param slice the buffer to read into, must be pre-sized to the required length
     * @param delta an offset into the slice buffer to start writing at
     * @return true if the requested bytes were read from a prefetched buffer, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean readBytes(long pos, ByteBuffer slice, int delta) throws IOException {
      final var entry = this.posToSlot.floorEntry(pos + delta);
      if (entry == null) {
        return false;
      }
      final int slot = entry.getValue();
      final long prefetchedPos = entry.getKey();
      // determine if the requested pos is within the prefetched range
      if (pos + delta >= prefetchedPos + prefetchBytesSize) {
        return false;
      }
      final Future<?> thread = prefetchThreads[slot];
      if (thread == null) {
        // free slot and decrement active prefetches
        clearSlotAndMaybeStartPending(slot);
        return false;
      }
      try {
        thread.get();
      } catch (ExecutionException | InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while waiting for prefetch", e);
      }
      if (prefetchExceptions[slot] != null) {
        IOException e = prefetchExceptions[slot];
        clearSlotAndMaybeStartPending(slot);
        throw e;
      }
      if (prefetchBuffers[slot] == null) {
        clearSlotAndMaybeStartPending(slot);
        return false;
      }
      // our buffer sizes are uniform, and match the required buffer size, however, the position
      // here
      // might be before the requested pos, so offset it
      slice.put(prefetchBuffers[slot]);
      slice.flip();
      slice.position(Math.toIntExact(pos - prefetchedPos) + delta);
      clearSlotAndMaybeStartPending(slot);
      return true;
    }

    void clearSlotAndMaybeStartPending(int slot) {
      prefetchExceptions[slot] = null;
      prefetchThreads[slot] = null;
      posToSlot.remove(prefetchPos[slot]);
      if (pendingPrefetches.isEmpty()) {
        slots.addLast(slot);
        return;
      }
      final long req = pendingPrefetches.removeFirst();
      posToSlot.put(req, slot);
      startPrefetch(req, slot);
    }

    void startPrefetch(long pos, int slot) {
      prefetchExceptions[slot] = null;
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  ByteBuffer prefetchBuffer = this.prefetchBuffers[slot];
                  if (prefetchBuffer == null) {
                    prefetchBuffer =
                        DirectIOIndexInput.allocateBuffer(prefetchBytesSize, blockSize);
                    this.prefetchBuffers[slot] = prefetchBuffer;
                  } else {
                    prefetchBuffer.clear();
                  }
                  channel.read(prefetchBuffer, pos);
                  prefetchBuffer.flip();
                } catch (IOException e) {
                  prefetchExceptions[slot] = e;
                }
              });
      prefetchThreads[slot] = future;
    }

    @Override
    public void close() throws IOException {
      executor.shutdownNow();
    }
  }
}
