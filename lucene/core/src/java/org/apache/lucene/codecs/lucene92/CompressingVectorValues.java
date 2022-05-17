package org.apache.lucene.codecs.lucene92;

import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class CompressingVectorValues extends FilterVectorValues {

  private final BytesRef binaryValue;
  private final double scale;

  /**
   * Sole constructor
   *
   * @param in
   */
  protected CompressingVectorValues(VectorValues in, double scale) {
    super(in);
    assert scale != 0;
    this.scale = scale;
    binaryValue = new BytesRef(in.dimension());
    binaryValue.length = in.dimension();
  }

  @Override
  public BytesRef binaryValue() throws IOException {
    float[] floats = vectorValue();
    byte[] bytes = binaryValue.bytes;
    for (int i = 0; i < dimension(); i++) {
      // scale and clip to [-128,127]
      bytes[i] = (byte) Math.max(-128, Math.min(127, Math.floor(floats[i] * scale)));
    }
    return binaryValue;
  }

}
