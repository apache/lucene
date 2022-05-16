package org.apache.lucene.codecs.lucene92;

import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class ExpandingRandomAccessVectorValues implements RandomAccessVectorValuesProducer {

  private final RandomAccessVectorValuesProducer delegate;
  private final float scale;

  /**
   * Wraps an existing vector values producer. Floating point vector values will be produced by scaling
   * byte-quantized values read from the values produced by the input.
   */
  protected ExpandingRandomAccessVectorValues(RandomAccessVectorValuesProducer in, float scale) {
    this.delegate = in;
    assert scale != 0;
    this.scale = scale;
  }

  @Override
  public RandomAccessVectorValues randomAccess() throws IOException {
    RandomAccessVectorValues delegateValues = delegate.randomAccess();
    float[] value  = new float[delegateValues.dimension()];;

    return new RandomAccessVectorValues() {

      @Override
      public int size() {
        return delegateValues.size();
      }

      @Override
      public int dimension() {
        return delegateValues.dimension();
      }

      @Override
      public float[] vectorValue(int targetOrd) throws IOException {
        BytesRef binaryValue = delegateValues.binaryValue(targetOrd);
        byte[] bytes = binaryValue.bytes;
        for (int i = 0, j = binaryValue.offset; i < value.length; i++, j++) {
          value[i] = bytes[j] * scale;
        }
        return value;
      }

      @Override
      public BytesRef binaryValue(int targetOrd) throws IOException {
        return delegateValues.binaryValue(targetOrd);
      }
    };
  }
}
