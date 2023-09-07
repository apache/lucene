package org.apache.lucene.codecs.lucene98;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;

/** The quantization state, includes lower and upper quantile */
class QuantizationState implements Accountable {
  private final float lowerQuantile, upperQuantile;

  static QuantizationState fromInput(IndexInput input) throws IOException {
    return new QuantizationState(
        Float.intBitsToFloat(input.readInt()), Float.intBitsToFloat(input.readInt()));
  }

  QuantizationState(float lowerQuantile, float upperQuantile) {
    this.lowerQuantile = lowerQuantile;
    this.upperQuantile = upperQuantile;
  }

  float getLowerQuantile() {
    return lowerQuantile;
  }

  float getUpperQuantile() {
    return upperQuantile;
  }

  void writeToOutput(IndexOutput output) throws IOException {
    output.writeInt(Float.floatToIntBits(lowerQuantile));
    output.writeInt(Float.floatToIntBits(upperQuantile));
  }

  @Override
  public long ramBytesUsed() {
    return Float.BYTES * 2;
  }
}
