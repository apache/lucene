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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link RateLimiter rate limiting} {@link IndexOutput}
 *
 * @lucene.internal
 */
public final class RateLimitedIndexOutput extends FilterIndexOutput {

  private final RateLimiter rateLimiter;

  /** How many bytes we've written since we last called rateLimiter.pause. */
  private final AtomicLong bytesSinceLastPause = new AtomicLong(0);

  /**
   * Cached here not not always have to call RateLimiter#getMinPauseCheckBytes() which does volatile
   * read.
   */
  private final AtomicLong currentMinPauseCheckBytes;

  public RateLimitedIndexOutput(final RateLimiter rateLimiter, final IndexOutput out) {
    super("RateLimitedIndexOutput(" + out + ")", out.getName(), out);
    this.rateLimiter = rateLimiter;
    this.currentMinPauseCheckBytes = new AtomicLong(rateLimiter.getMinPauseCheckBytes());
  }

  @Override
  public void writeByte(byte b) throws IOException {
    bytesSinceLastPause.incrementAndGet();
    checkRate();
    out.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    bytesSinceLastPause.addAndGet(length);
    checkRate();
    // The bytes array slice is written without pauses.
    // This can cause instant write rate to breach rate limit if there have
    // been no writes for enough time to keep the average write rate within limit.
    // See https://issues.apache.org/jira/browse/LUCENE-10448
    out.writeBytes(b, offset, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    bytesSinceLastPause.addAndGet(Integer.BYTES);
    checkRate();
    out.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    bytesSinceLastPause.addAndGet(Short.BYTES);
    checkRate();
    out.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    bytesSinceLastPause.addAndGet(Long.BYTES);
    checkRate();
    out.writeLong(i);
  }

  private void checkRate() throws IOException {
    AtomicLong localBytesSinceLastPause = new AtomicLong(0);
    AtomicBoolean shouldPause = new AtomicBoolean(false);
    bytesSinceLastPause.updateAndGet(
        bytes -> {
          if (bytes > currentMinPauseCheckBytes.get()) {
            shouldPause.set(true);
            currentMinPauseCheckBytes.set(rateLimiter.getMinPauseCheckBytes());
            localBytesSinceLastPause.set(bytes);
            return 0;
          } else {
            shouldPause.set(false);
          }
          return bytes;
        });
    if (shouldPause.get()) {
      rateLimiter.pause(localBytesSinceLastPause.get());
    }
  }
}
