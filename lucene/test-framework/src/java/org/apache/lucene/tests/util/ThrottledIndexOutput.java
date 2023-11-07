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
package org.apache.lucene.tests.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ThreadInterruptedException;

/** Intentionally slow IndexOutput for testing. */
public class ThrottledIndexOutput extends FilterIndexOutput {
  public static final int DEFAULT_MIN_WRITTEN_BYTES = 1024;
  private final int bytesPerSecond;
  private long flushDelayMillis;
  private long closeDelayMillis;
  private long seekDelayMillis;
  private long pendingBytes;
  private long minBytesWritten;
  private long timeElapsed;
  private final byte[] bytes = new byte[1];

  public ThrottledIndexOutput newFromDelegate(IndexOutput out) {
    return new ThrottledIndexOutput(
        bytesPerSecond, flushDelayMillis, closeDelayMillis, seekDelayMillis, minBytesWritten, out);
  }

  public ThrottledIndexOutput(int bytesPerSecond, long delayInMillis, IndexOutput out) {
    this(
        bytesPerSecond,
        delayInMillis,
        delayInMillis,
        delayInMillis,
        DEFAULT_MIN_WRITTEN_BYTES,
        out);
  }

  public ThrottledIndexOutput(
      int bytesPerSecond, long delays, int minBytesWritten, IndexOutput out) {
    this(bytesPerSecond, delays, delays, delays, minBytesWritten, out);
  }

  public static final int mBitsToBytes(int mbits) {
    return mbits * 125000000;
  }

  public ThrottledIndexOutput(
      int bytesPerSecond,
      long flushDelayMillis,
      long closeDelayMillis,
      long seekDelayMillis,
      long minBytesWritten,
      IndexOutput out) {
    super("ThrottledIndexOutput(" + out + ")", out == null ? "n/a" : out.getName(), out);
    assert bytesPerSecond > 0;
    this.bytesPerSecond = bytesPerSecond;
    this.flushDelayMillis = flushDelayMillis;
    this.closeDelayMillis = closeDelayMillis;
    this.seekDelayMillis = seekDelayMillis;
    this.minBytesWritten = minBytesWritten;
  }

  @Override
  public void close() throws IOException {
    try {
      sleep(closeDelayMillis + getDelay(true));
    } finally {
      out.close();
    }
  }

  @Override
  public void writeByte(byte b) throws IOException {
    bytes[0] = b;
    writeBytes(bytes, 0, 1);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    final long before = System.nanoTime();
    // TODO: sometimes, write only half the bytes, then
    // sleep, then 2nd half, then sleep, so we sometimes
    // interrupt having only written not all bytes
    out.writeBytes(b, offset, length);
    timeElapsed += System.nanoTime() - before;
    pendingBytes += length;
    sleep(getDelay(false));
  }

  protected long getDelay(boolean closing) {
    if (pendingBytes > 0 && (closing || pendingBytes > minBytesWritten)) {
      long actualBps = (timeElapsed / pendingBytes) * 1000000000l; // nano to sec
      if (actualBps > bytesPerSecond) {
        long expected = (pendingBytes * 1000l / bytesPerSecond);
        final long delay = expected - TimeUnit.NANOSECONDS.toMillis(timeElapsed);
        pendingBytes = 0;
        timeElapsed = 0;
        return delay;
      }
    }
    return 0;
  }

  private static void sleep(long ms) {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }
}
