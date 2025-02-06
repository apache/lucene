package org.apache.lucene.index;

import java.util.Objects;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;

public class StoredFieldDataInput {

  private final DataInput in;
  private final int length;

  public StoredFieldDataInput(DataInput in, int length) {
    this.in = Objects.requireNonNull(in);
    this.length = length;
  }

  public StoredFieldDataInput(ByteArrayDataInput byteArrayDataInput) {
    this(byteArrayDataInput, byteArrayDataInput.length());
  }

  public DataInput getDataInput() {
    return in;
  }

  public int getLength() {
    return length;
  }
}
