package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

class EndiannessReverserDataOutput extends DataOutput {

  private final DataOutput out;

  EndiannessReverserDataOutput(DataOutput out) {
    this.out = out;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    out.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int length) throws IOException {
    out.writeBytes(b, length);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    out.writeBytes(b, offset, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    out.writeInt(Integer.reverseBytes(i));
  }

  @Override
  public void writeShort(short i) throws IOException {
    out.writeShort(Short.reverseBytes(i));
  }

  @Override
  public void writeLong(long i) throws IOException {
    out.writeLong(Long.reverseBytes(i));
  }

  @Override
  public void writeString(String s) throws IOException {
    out.writeString(s);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    out.copyBytes(input, numBytes);
  }

  @Override
  public void writeMapOfStrings(Map<String, String> map) throws IOException {
    out.writeMapOfStrings(map);
  }

  @Override
  public void writeSetOfStrings(Set<String> set) throws IOException {
    out.writeSetOfStrings(set);
  }
}
