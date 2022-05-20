package org.apache.lucene.codecs.lucene92;

import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * convert from floating point to reduced-precision byte
 */
public class CompressingVectorValues extends FilterVectorValues {

  private final BytesRef binaryValue;

  /**
   * Sole constructor
   *
   * @param in
   */
  protected CompressingVectorValues(VectorValues in) {
    super(in);
    binaryValue = new BytesRef(in.dimension());
    binaryValue.length = in.dimension();
  }

  @Override
  public BytesRef binaryValue() throws IOException {
    float[] floats = vectorValue();
    byte[] bytes = binaryValue.bytes;
    for (int i = 0; i < dimension(); i++) {
      // scale and clip to [-128,127]
      bytes[i] = (byte) floats[i];
    }
    return binaryValue;
  }

}
