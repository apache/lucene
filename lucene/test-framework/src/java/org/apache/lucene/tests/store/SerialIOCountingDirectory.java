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
package org.apache.lucene.tests.store;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.CloseableThreadLocal;

/**
 * A {@link Directory} wrapper that counts the number of times that Lucene may wait for I/O to
 * return serially. Lower counts mean that Lucene better takes advantage of I/O parallelism.
 */
public class SerialIOCountingDirectory extends FilterDirectory {

  private static final long PAGE_SHIFT = 12; // 4096 bytes per page
  // Assumed number of pages that are read ahead
  private static final int PAGE_READAHEAD = 4;
  private final LongAdder counter = new LongAdder();
  private final CloseableThreadLocal<Boolean> pendingFetch =
      new CloseableThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return Boolean.FALSE;
        }
      };

  /** Sole constructor. */
  public SerialIOCountingDirectory(Directory in) {
    super(in);
  }

  @Override
  public void close() throws IOException {
    pendingFetch.close();
    super.close();
  }

  /** Return the number of I/O request performed serially. */
  public long count() {
    return counter.sum();
  }

  @Override
  public ChecksumIndexInput openChecksumInput(String name) throws IOException {
    // sequential access, count 1 for the whole file
    counter.increment();
    return super.openChecksumInput(name);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (context.readAdvice() == ReadAdvice.RANDOM_PRELOAD) {
      // expected to be loaded in memory, only count 1 at open time
      counter.increment();
      return super.openInput(name, context);
    }
    return new SerializedIOCountingIndexInput(super.openInput(name, context), context.readAdvice());
  }

  private class SerializedIOCountingIndexInput extends IndexInput {

    private final IndexInput in;
    private final long sliceOffset, sliceLength;
    private final ReadAdvice readAdvice;
    private final LongHashSet pendingPages = new LongHashSet();
    private long currentPage = Long.MIN_VALUE;

    public SerializedIOCountingIndexInput(IndexInput in, ReadAdvice readAdvice) {
      this(in, readAdvice, 0L, in.length());
    }

    public SerializedIOCountingIndexInput(
        IndexInput in, ReadAdvice readAdvice, long offset, long length) {
      super(in.toString());
      this.in = in;
      this.sliceOffset = offset;
      this.sliceLength = length;
      this.readAdvice = readAdvice;
    }

    private void onRead(long offset, int len) {
      if (len == 0) {
        return;
      }
      final long firstPage = (sliceOffset + offset) >> PAGE_SHIFT;
      final long lastPage = (sliceOffset + offset + len - 1) >> PAGE_SHIFT;

      for (long page = firstPage; page <= lastPage; ++page) {
        long readAheadUpto;
        if (readAdvice == ReadAdvice.RANDOM) {
          readAheadUpto = currentPage;
        } else {
          // Assume that the next few pages are always free to read thanks to read-ahead.
          readAheadUpto = currentPage + PAGE_READAHEAD;
        }

        if (pendingPages.contains(page) == false && (page < currentPage || page > readAheadUpto)) {
          counter.increment();
        }
        currentPage = page;
      }
      pendingFetch.set(false);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
      final long firstPage = (sliceOffset + offset) >> PAGE_SHIFT;
      final long lastPage = (sliceOffset + offset + length - 1) >> PAGE_SHIFT;

      long readAheadUpto;
      if (readAdvice == ReadAdvice.RANDOM) {
        readAheadUpto = currentPage;
      } else {
        // Assume that the next few pages are always free to read thanks to read-ahead.
        readAheadUpto = currentPage + PAGE_READAHEAD;
      }

      if (firstPage >= currentPage && lastPage <= readAheadUpto) {
        // seeking within the current (or next page if ReadAdvice.NORMAL) doesn't increment the
        // counter
      } else if (pendingFetch.get() == false) {
        // If multiple prefetch calls are performed without a readXXX() call in-between, count a
        // single increment as these I/O requests can be performed in parallel.
        counter.increment();
        pendingPages.clear();
        pendingFetch.set(true);
      }

      for (long page = firstPage; page <= lastPage; ++page) {
        pendingPages.add(page);
      }
    }

    @Override
    public byte readByte() throws IOException {
      onRead(getFilePointer(), Byte.BYTES);
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      onRead(getFilePointer(), len);
      in.readBytes(b, offset, len);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer() - sliceOffset;
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(sliceOffset + pos);
    }

    @Override
    public long length() {
      return sliceLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return slice(sliceDescription, offset, length, readAdvice);
    }

    @Override
    public IndexInput slice(
        String sliceDescription, long offset, long length, ReadAdvice readAdvice)
        throws IOException {
      if (offset < 0 || offset + length > sliceLength) {
        throw new IllegalArgumentException();
      }
      IndexInput clone = in.clone();
      clone.seek(sliceOffset + offset);
      return new SerializedIOCountingIndexInput(clone, readAdvice, sliceOffset + offset, length);
    }

    @Override
    public IndexInput clone() {
      IndexInput clone = in.clone();
      return new SerializedIOCountingIndexInput(clone, readAdvice, sliceOffset, sliceLength);
    }

    @Override
    public Optional<Boolean> isLoaded() {
      return in.isLoaded();
    }
  }
}
