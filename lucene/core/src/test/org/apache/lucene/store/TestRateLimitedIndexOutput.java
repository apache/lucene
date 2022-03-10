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
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestRateLimitedIndexOutput extends LuceneTestCase {

  public void testWriteBytesChunking() throws Exception {
    MockIndexOutputDelegate delegate = new MockIndexOutputDelegate();
    final int minPauseCheckBytes = 10;
    final int chunk = minPauseCheckBytes + 1;
    MockRateLimiter rateLimiter = new MockRateLimiter(minPauseCheckBytes);
    RateLimitedIndexOutput output = new RateLimitedIndexOutput(rateLimiter, delegate);

    final int startOffset = 4;
    final int length = 90;
    byte[] buf = new byte[length];
    output.writeBytes(buf, startOffset, length);
    assertEquals(length / chunk, rateLimiter.pauseCallCount);
    assertEquals(length, delegate.bytesWritten);
    assertEquals(startOffset + length, delegate.endOffset);
  }

  public void testNegativeChunkSize() {
    IndexOutput delegate = new MockIndexOutputDelegate();
    final int minPauseCheckBytes = -1;
    MockRateLimiter rateLimiter = new MockRateLimiter(minPauseCheckBytes);
    RateLimitedIndexOutput output = new RateLimitedIndexOutput(rateLimiter, delegate);
    final int length = 90;
    byte[] buf = new byte[length];
    assertThrows(AssertionError.class, () -> output.writeBytes(buf, 0, length));
  }

  public static class MockIndexOutputDelegate extends IndexOutput {

    public int bytesWritten = 0;
    public int endOffset = 0;

    public MockIndexOutputDelegate() {
      super("mock delegate for tests", "test-index-output");
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
      return 0;
    }

    @Override
    public long getChecksum() throws IOException {
      return 0;
    }

    @Override
    public void writeByte(byte b) throws IOException {}

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      bytesWritten += length;
      endOffset = offset + length;
    }
  }

  public static class MockRateLimiter extends RateLimiter {
    public int pauseCallCount = 0;

    private double mbPerSec;
    private long minPauseCheckBytes;

    public MockRateLimiter(long minPauseCheckBytes) {
      this.minPauseCheckBytes = minPauseCheckBytes;
    }

    @Override
    public void setMBPerSec(double mbPerSec) {
      this.mbPerSec = mbPerSec;
    }

    @Override
    public double getMBPerSec() {
      return mbPerSec;
    }

    @Override
    public long pause(long bytes) throws IOException {
      pauseCallCount++;
      return 0;
    }

    @Override
    public long getMinPauseCheckBytes() {
      return minPauseCheckBytes;
    }
  }
}
