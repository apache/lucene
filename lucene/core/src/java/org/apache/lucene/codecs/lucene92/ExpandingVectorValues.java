package org.apache.lucene.codecs.lucene92;

import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * reads from byte-encoded data
 */
public class ExpandingVectorValues extends FilterVectorValues {

  private final float[] value;
  private final float scale;

  /**
   * Sole constructor
   *
   * @param in
   */
  protected ExpandingVectorValues(VectorValues in, float scale) {
    super(in);
    assert scale != 0;
    this.scale = scale;
    value = new float[in.dimension()];
  }

  @Override
  public float[] vectorValue() throws IOException {
    BytesRef binaryValue = binaryValue();
    byte[] bytes = binaryValue.bytes;
    for (int i = 0, j = binaryValue.offset; i < value.length; i++, j++) {
      value[i] = bytes[j] * scale;
    }
    return value;
  }
}
