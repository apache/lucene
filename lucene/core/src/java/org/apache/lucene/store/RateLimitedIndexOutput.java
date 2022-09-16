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

/**
 * A {@link RateLimiter rate limiting} {@link IndexOutput}
 *
 * @lucene.internal
 */
public final class RateLimitedIndexOutput extends IndexOutput {

  private final IndexOutput delegate;
  private final RateLimiter rateLimiter;

  /** How many bytes we've written since we last called rateLimiter.pause. */
  private long bytesSinceLastPause;

  /**
   * Cached here not not always have to call RateLimiter#getMinPauseCheckBytes() which does volatile
   * read.
   */
  private long currentMinPauseCheckBytes;

  public RateLimitedIndexOutput(final RateLimiter rateLimiter, final IndexOutput delegate) {
    super("RateLimitedIndexOutput(" + delegate + ")", delegate.getName());
    this.delegate = delegate;
    this.rateLimiter = rateLimiter;
    this.currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public long getFilePointer() {
    return delegate.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return delegate.getChecksum();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    bytesSinceLastPause++;
    checkRate();
    delegate.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    bytesSinceLastPause += length;
    checkRate();
    // The bytes array slice is written without pauses.
    // This can cause instant write rate to breach rate limit if there have
    // been no writes for enough time to keep the average write rate within limit.
    // See https://issues.apache.org/jira/browse/LUCENE-10448
    delegate.writeBytes(b, offset, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    bytesSinceLastPause += Integer.BYTES;
    checkRate();
    delegate.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    bytesSinceLastPause += Short.BYTES;
    checkRate();
    delegate.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    bytesSinceLastPause += Long.BYTES;
    checkRate();
    delegate.writeLong(i);
  }

  private void checkRate() throws IOException {
    if (bytesSinceLastPause > currentMinPauseCheckBytes) {
      rateLimiter.pause(bytesSinceLastPause);
      bytesSinceLastPause = 0;
      currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();
    }
  }
}
