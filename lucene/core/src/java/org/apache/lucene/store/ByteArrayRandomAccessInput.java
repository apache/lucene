package org.apache.lucene.store;

import java.io.IOException;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * RandomAccessInput implementation backed by a byte array. <b>WARNING:</b> This class omits most
 * low-level checks, so be sure to test heavily with assertions enabled.
 *
 * @lucene.experimental
 */
public final class ByteArrayRandomAccessInput implements RandomAccessInput {

  private byte[] bytes;

  public ByteArrayRandomAccessInput(byte[] bytes) {
    this.bytes = bytes;
  }

  public ByteArrayRandomAccessInput() {
    this(BytesRef.EMPTY_BYTES);
  }

  /** Reset the array to the provided one so it can be reused */
  public void reset(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public long length() {
    return bytes.length;
  }

  @Override
  public byte readByte(long pos) throws IOException {
    return bytes[(int) pos];
  }

  @Override
  public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
    System.arraycopy(this.bytes, (int) pos, bytes, offset, length);
  }

  @Override
  public short readShort(long pos) throws IOException {
    return (short) BitUtil.VH_LE_SHORT.get(bytes, (int) pos);
  }

  @Override
  public int readInt(long pos) throws IOException {
    return (int) BitUtil.VH_LE_INT.get(bytes, (int) pos);
  }

  @Override
  public long readLong(long pos) throws IOException {
    return (long) BitUtil.VH_LE_LONG.get(bytes, (int) pos);
  }
}
