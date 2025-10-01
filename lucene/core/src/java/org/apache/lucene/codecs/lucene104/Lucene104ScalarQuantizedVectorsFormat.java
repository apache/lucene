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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * The quantization format used here is a per-vector optimized scalar quantization. These ideas are
 * evolutions of LVQ proposed in <a href="https://arxiv.org/abs/2304.04759">Similarity search in the
 * blink of an eye with compressed indices</a> by Cecilia Aguerrebere et al., the previous work on
 * globally optimized scalar quantization in Apache Lucene, and <a
 * href="https://arxiv.org/abs/1908.10396">Accelerating Large-Scale Inference with Anisotropic
 * Vector Quantization </a> by Ruiqi Guo et. al. Also see {@link
 * org.apache.lucene.util.quantization.OptimizedScalarQuantizer}. Some of key features are:
 *
 * <ul>
 *   <li>Estimating the distance between two vectors using their centroid centered distance. This
 *       requires some additional corrective factors, but allows for centroid centering to occur.
 *   <li>Optimized scalar quantization to single bit level of centroid centered vectors.
 *   <li>Asymmetric quantization of vectors, where query vectors are quantized to half-byte (4 bits)
 *       precision (normalized to the centroid) and then compared directly against the single bit
 *       quantized vectors in the index.
 *   <li>Transforming the half-byte quantized query vectors in such a way that the comparison with
 *       single bit vectors can be done with bit arithmetic.
 * </ul>
 *
 * A previous work related to improvements over regular LVQ is <a
 * href="https://arxiv.org/abs/2409.09913">Practical and Asymptotically Optimal Quantization of
 * High-Dimensional Vectors in Euclidean Space for Approximate Nearest Neighbor Search </a> by
 * Jianyang Gao, et. al.
 *
 * <p>The format is stored within two files:
 *
 * <h2>.veq (vector data) file</h2>
 *
 * <p>Stores the quantized vectors in a flat format. Additionally, it stores each vector's
 * corrective factors. At the end of the file, additional information is stored for vector ordinal
 * to centroid ordinal mapping and sparse vector information.
 *
 * <ul>
 *   <li>For each vector:
 *       <ul>
 *         <li><b>[byte]</b> the quantized values. Each dimension may be up to 8 bits, and multiple
 *             dimensions may be packed into a single byte.
 *         <li><b>[float]</b> the optimized quantiles and an additional similarity dependent
 *             corrective factor.
 *         <li><b>[int]</b> the sum of the quantized components
 *       </ul>
 *   <li>After the vectors, sparse vector information keeping track of monotonic blocks.
 * </ul>
 *
 * <h2>.vemq (vector metadata) file</h2>
 *
 * <p>Stores the metadata for the vectors. This includes the number of vectors, the number of
 * dimensions, and file offset information.
 *
 * <ul>
 *   <li><b>int</b> the field number
 *   <li><b>int</b> the vector encoding ordinal
 *   <li><b>int</b> the vector similarity ordinal
 *   <li><b>vint</b> the vector dimensions
 *   <li><b>vlong</b> the offset to the vector data in the .veq file
 *   <li><b>vlong</b> the length of the vector data in the .veq file
 *   <li><b>vint</b> the number of vectors
 *   <li><b>vint</b> the wire number for ScalarEncoding
 *   <li><b>[float]</b> the centroid
 *   <li><b>float</b> the centroid square magnitude
 *   <li>The sparse vector information, if required, mapping vector ordinal to doc ID
 * </ul>
 */
public class Lucene104ScalarQuantizedVectorsFormat extends FlatVectorsFormat {
  public static final String QUANTIZED_VECTOR_COMPONENT = "QVEC";
  public static final String NAME = "Lucene104ScalarQuantizedVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "Lucene104ScalarQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene104ScalarQuantizedVectorsFormatData";
  static final String META_EXTENSION = "vemq";
  static final String VECTOR_DATA_EXTENSION = "veq";
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private static final Lucene104ScalarQuantizedVectorScorer scorer =
      new Lucene104ScalarQuantizedVectorScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private final ScalarEncoding encoding;

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
    SINGLE_BIT_QUERY_NIBBLE(3, (byte) 1, 1, (byte) 4, 4);

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

    boolean isAsymmetric() {
      return bits != queryBits;
    }

    int getWireNumber() {
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

  /** Creates a new instance with UNSIGNED_BYTE encoding. */
  public Lucene104ScalarQuantizedVectorsFormat() {
    this(ScalarEncoding.UNSIGNED_BYTE);
  }

  /** Creates a new instance with the chosen quantization encoding. */
  public Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding encoding) {
    super(NAME);
    this.encoding = encoding;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene104ScalarQuantizedVectorsWriter(
        state, encoding, rawVectorFormat.fieldsWriter(state), scorer);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene104ScalarQuantizedVectorsReader(
        state, rawVectorFormat.fieldsReader(state), scorer);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  @Override
  public String toString() {
    return "Lucene104ScalarQuantizedVectorsFormat(name="
        + NAME
        + ", encoding="
        + encoding
        + ", flatVectorScorer="
        + scorer
        + ", rawVectorFormat="
        + rawVectorFormat
        + ")";
  }
}
