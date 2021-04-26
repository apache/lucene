package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import org.apache.lucene.store.DataInput;

class EndiannessReverserDataInput extends DataInput {

  private final DataInput in;

  EndiannessReverserDataInput(DataInput in) {
    this.in = in;
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    in.readBytes(b, offset, len);
  }

  @Override
  public short readShort() throws IOException {
    return Short.reverseBytes(in.readShort());
  }

  @Override
  public int readInt() throws IOException {
    return Integer.reverseBytes(in.readInt());
  }

  @Override
  public long readLong() throws IOException {
    return Long.reverseBytes(in.readLong());
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    in.skipBytes(numBytes);
  }
}
