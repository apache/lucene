package org.apache.lucene.store;

import org.apache.lucene.util.BitUtil;

/**
 * An extension of the ByteArrayDataInput to support reading the input byte array in a circular
 * manner. This means that if the position goes beyond the byte array's length, the internal
 * position is reinitialized to zero.
 */
public final class ByteArrayCircularDataInput extends ByteArrayDataInput {

  private final int initPos;
  private boolean hasLoopedBack = false;

  public ByteArrayCircularDataInput(byte[] bytes) {
    super(bytes);
    this.initPos = pos;
  }

  public ByteArrayCircularDataInput(byte[] bytes, int offset, int len) {
    super(bytes, offset, len);
    this.initPos = pos;
  }

  public ByteArrayCircularDataInput() {
    super();
    this.initPos = pos;
  }

  /**
   * @return the relative position (can be larger than the byte array length)
   */
  @Override
  public int getPosition() {

    return hasLoopedBack ? pos + bytes.length : super.getPosition();
  }

  /**
   * Set a new relative position (can be larger than the byte array length)
   *
   * @param pos the new position
   */
  @Override
  public void setPosition(int pos) {

    if (pos < bytes.length) super.setPosition(pos);
    else this.pos = pos - bytes.length;
  }

  /**
   * @return true if the byte array has been read past the specified number of bytes
   */
  @Override
  public boolean eof() {

    return hasLoopedBack ? (pos == initPos) : super.eof();
  }

  private interface valueGenFunc<T> {
    T gen();
  }

  private interface valueGenFromBytesFunc<T> {
    T gen(byte[] arr);
  }

  private <T extends Number> T readValueWithLoopBack(
      int typeSize, valueGenFunc<T> superFn, valueGenFromBytesFunc<T> readFn) {

    // if the position does not go beyond the bytes buffer rely on parent method
    if (pos + typeSize < bytes.length) {
      return superFn.gen();
    }

    // else build a byte array specifically for this read, joining the pieces
    byte[] byteArr = new byte[typeSize];
    int cnt = 0;
    while (pos < bytes.length) {
      byteArr[cnt++] = bytes[pos++];
    }
    pos = 0;
    hasLoopedBack = true;
    while (cnt < typeSize) {
      byteArr[cnt++] = bytes[pos++];
    }
    return readFn.gen(byteArr);
  }

  @Override
  public short readShort() {

    return readValueWithLoopBack(
        Short.BYTES, () -> super.readShort(), (arr) -> (short) BitUtil.VH_LE_SHORT.get(arr, 0));
  }

  @Override
  public int readInt() {

    return readValueWithLoopBack(
        Integer.BYTES, () -> super.readInt(), (arr) -> (int) BitUtil.VH_LE_INT.get(arr, 0));
  }

  @Override
  public long readLong() {

    return readValueWithLoopBack(
        Long.BYTES, () -> super.readLong(), (arr) -> (long) BitUtil.VH_LE_LONG.get(arr, 0));
  }

  @Override
  public byte readByte() {
    byte b = super.readByte();
    if (pos >= bytes.length) {
      hasLoopedBack = true;
      pos = 0;
    }
    return b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    if (pos + len < bytes.length) {
      super.readBytes(b, offset, len);
      if (pos >= bytes.length) {
        hasLoopedBack = true;
        pos = 0;
      }
    } else {
      int remaining = len;
      if (pos < bytes.length) {
        System.arraycopy(bytes, pos, b, offset, bytes.length - pos);
        remaining -= bytes.length - pos;
        offset += bytes.length - pos;
      }
      pos = 0;
      hasLoopedBack = true;
      System.arraycopy(bytes, pos, b, offset, remaining);
      pos += remaining;
    }
  }
}
