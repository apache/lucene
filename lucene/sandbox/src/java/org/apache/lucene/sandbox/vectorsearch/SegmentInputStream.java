package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import java.io.InputStream;

import org.apache.lucene.store.IndexInput;

public class SegmentInputStream extends InputStream {

  /**
   * 
   */
  private final IndexInput indexInput;
  public final long initialFilePointerPosition;
  public final long limit;
  public long pos = 0;

  // TODO: This input stream needs to be modified to enable buffering.
  public SegmentInputStream(IndexInput indexInput, long limit, long initialFilePointerPosition) throws IOException {
    super();
    this.indexInput = indexInput;
    this.initialFilePointerPosition = initialFilePointerPosition;
    this.limit = limit;

    this.indexInput.seek(initialFilePointerPosition);
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b, int off, int len) {
    try {
      long avail = limit - pos;
      if (pos >= limit) {
        return -1;
      }
      if (len > avail) {
        len = (int) avail;
      }
      if (len <= 0) {
        return 0;
      }
      indexInput.readBytes(b, off, len);
      pos += len;
      return len;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() throws IOException {
    indexInput.seek(initialFilePointerPosition);
    pos = 0;
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // Do nothing for now.
  }

  @Override
  public int available() {
    throw new UnsupportedOperationException();
  }

}