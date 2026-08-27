/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.quantization;

import java.io.IOException;
import java.util.Optional;

/**
 * Scalar quantized byte vector values
 *
 * @lucene.experimental
 */
public abstract class QuantizedByteVectorValues extends BaseQuantizedByteVectorValues {

  /**
   * Allowed encodings for scalar quantization.
   *
   * <p>This specifies how many bits are used per dimension and also dictates packing of dimensions
   * into a byte stream.
   */
  public enum ScalarEncoding {
    /** Each dimension is quantized to 8 bits and treated as an unsigned value. */
    UNSIGNED_BYTE(0, (byte) 8, 8),
    /** Each dimension is quantized to 4 bits two values are packed into each output byte. */
    PACKED_NIBBLE(1, (byte) 4, 4),
    /**
     * Each dimension is quantized to 7 bits and treated as a signed value.
     *
     * <p>This is intended for backwards compatibility with older iterations of scalar quantization.
     * This setting will produce an index the same size as {@link #UNSIGNED_BYTE} but will produce
     * less accurate vector comparisons.
     */
    SEVEN_BIT(2, (byte) 7, 8),
    /**
     * Each dimension is quantized to a single bit and packed into bytes. During query time, the
     * query vector is quantized to 4 bits per dimension.
     *
     * <p>This is the most space efficient encoding, and will produce an index 8x smaller than
     * {@link #UNSIGNED_BYTE}. However, this comes at the cost of accuracy.
     */
    SINGLE_BIT_QUERY_NIBBLE(3, (byte) 1, 1, (byte) 4, 4),
    /**
     * Each dimension is quantized to 2 bits (dibit) and packed into bytes. During query time, the
     * query vector is quantized to 4 bits per dimension.
     *
     * <p>This encoding produces an index 4x smaller than {@link #UNSIGNED_BYTE}, offering a balance
     * between the compression of {@link #SINGLE_BIT_QUERY_NIBBLE} and the accuracy of {@link
     * #PACKED_NIBBLE}.
     */
    DIBIT_QUERY_NIBBLE(4, (byte) 2, 2, (byte) 4, 4) {
      @Override
      public int getDiscreteDimensions(int dimensions) {
        int queryDiscretized = (dimensions * 4 + 7) / 8 * 8 / 4;
        // we want to force dibit packing to byte boundaries assuming single bit striping
        // so we discretize to the same as single bit encoding
        int docDiscretized = (dimensions + 7) / 8 * 8;
        int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
        assert maxDiscretized % (8.0 / 4) == 0
            : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        assert maxDiscretized % (8.0 / 2) == 0
            : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        return maxDiscretized;
      }

      @Override
      public int getDocPackedLength(int dimensions) {
        int discretized = getDiscreteDimensions(dimensions);
        // DIBIT should be stored as two single bits striped
        return 2 * ((discretized + 7) / 8);
      }
    };

    public static ScalarEncoding fromNumBits(int bits) {
      for (ScalarEncoding encoding : values()) {
        if (encoding.bits == bits) {
          return encoding;
        }
      }
      throw new IllegalArgumentException("No encoding for " + bits + " bits");
    }

    /** The number used to identify this encoding on the wire, rather than relying on ordinal. */
    private final int wireNumber;

    private final byte bits, queryBits;
    private final int bitsPerDim, queryBitsPerDim;

    ScalarEncoding(int wireNumber, byte bits, int bitsPerDim) {
      this.wireNumber = wireNumber;
      this.bits = bits;
      this.queryBits = bits;
      this.bitsPerDim = bitsPerDim;
      this.queryBitsPerDim = bitsPerDim;
    }

    ScalarEncoding(int wireNumber, byte bits, int bitsPerDim, byte queryBits, int queryBitsPerDim) {
      this.wireNumber = wireNumber;
      this.bits = bits;
      this.queryBits = queryBits;
      this.bitsPerDim = bitsPerDim;
      this.queryBitsPerDim = queryBitsPerDim;
    }

    public boolean isAsymmetric() {
      return bits != queryBits;
    }

    public int getWireNumber() {
      return wireNumber;
    }

    /** Return the number of bits used per dimension. */
    public byte getBits() {
      return bits;
    }

    public byte getQueryBits() {
      return queryBits;
    }

    /** Return the number of dimensions rounded up to fit into whole bytes. */
    public int getDiscreteDimensions(int dimensions) {
      if (queryBits == bits) {
        int totalBits = dimensions * bitsPerDim;
        return (totalBits + 7) / 8 * 8 / bitsPerDim;
      }
      int queryDiscretized = (dimensions * queryBitsPerDim + 7) / 8 * 8 / queryBitsPerDim;
      int docDiscretized = (dimensions * bitsPerDim + 7) / 8 * 8 / bitsPerDim;
      int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
      assert maxDiscretized % (8.0 / queryBitsPerDim) == 0
          : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
      assert maxDiscretized % (8.0 / bitsPerDim) == 0
          : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
      return maxDiscretized;
    }

    /** Return the number of dimensions that can be packed into a single byte. */
    public int getDocBitsPerDim() {
      return this.bitsPerDim;
    }

    public int getQueryBitsPerDim() {
      return this.queryBitsPerDim;
    }

    /** Return the number of bytes required to store a packed vector of the given dimensions. */
    public int getDocPackedLength(int dimensions) {
      int discretized = getDiscreteDimensions(dimensions);
      // how many bytes do we need to store the quantized vector?
      int totalBits = discretized * bitsPerDim;
      return (totalBits + 7) / 8;
    }

    public int getQueryPackedLength(int dimensions) {
      int discretized = getDiscreteDimensions(dimensions);
      // how many bytes do we need to store the quantized vector?
      int totalBits = discretized * queryBitsPerDim;
      return (totalBits + 7) / 8;
    }

    /** Returns the encoding for the given wire number, or empty if unknown. */
    public static Optional<ScalarEncoding> fromWireNumber(int wireNumber) {
      for (ScalarEncoding encoding : values()) {
        if (encoding.wireNumber == wireNumber) {
          return Optional.of(encoding);
        }
      }
      return Optional.empty();
    }
  }

  /**
   * Retrieve the corrective terms for the given vector ordinal. For the dot-product family of
   * distances, the corrective terms are, in order
   *
   * <ul>
   *   <li>the lower optimized interval
   *   <li>the upper optimized interval
   *   <li>the dot-product of the non-centered vector with the centroid
   *   <li>the sum of quantized components
   * </ul>
   *
   * For euclidean:
   *
   * <ul>
   *   <li>the lower optimized interval
   *   <li>the upper optimized interval
   *   <li>the l2norm of the centered vector
   *   <li>the sum of quantized components
   * </ul>
   *
   * @param vectorOrd the vector ordinal
   * @return the corrective terms
   * @throws IOException if an I/O error occurs
   */
  public abstract OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd)
      throws IOException;

  /**
   * @return the quantizer used to quantize the vectors
   */
  public abstract OptimizedScalarQuantizer getQuantizer();

  /**
   * @return the scalar encoding used to pack the stored vectors.
   */
  public abstract ScalarEncoding getScalarEncoding();

  /**
   * @return the centroid used to center the vectors prior to quantization
   */
  public abstract float[] getCentroid() throws IOException;

  /**
   * @return the dot product of the centroid.
   */
  public abstract float getCentroidDP() throws IOException;

  @Override
  public abstract QuantizedByteVectorValues copy() throws IOException;
}
