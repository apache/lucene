package org.apache.lucene.store;

import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

public class TestRateLimitedIndexOutput extends LuceneTestCase {

  public void testWriteBytesChunking() throws Exception {

    IndexOutput delegate = new IndexOutput("mock delegate for tests", "test-index-output") {
      @Override
      public void close() throws IOException {
      }

      @Override
      public long getFilePointer() {
        return 0;
      }

      @Override
      public long getChecksum() throws IOException {
        return 0;
      }

      @Override
      public void writeByte(byte b) throws IOException {
      }

      @Override
      public void writeBytes(byte[] b, int offset, int length) throws IOException {

      }
    };

    final int minPauseCheckBytes = 10;
    final int chunk = minPauseCheckBytes + 1;
    MockRateLimiter rateLimiter = new MockRateLimiter(minPauseCheckBytes);
    RateLimitedIndexOutput output = new RateLimitedIndexOutput(rateLimiter, delegate);

    final int length = 90;
    byte[] buf = new byte[length];
    output.writeBytes(buf, 0, length);
    assertEquals(length / chunk, rateLimiter.pauseCallCount);
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
