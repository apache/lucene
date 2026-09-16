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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Write-side helper performing <i>data-blind</i> scalar quantization of de-duplicated vectors.
 *
 * <p>Quantization assumes input vectors are evenly distributed around the origin, i.e. the centroid
 * is a zero vector. The quantized record of a vector (packed bytes, optimized intervals, an
 * additional correction and the component sum) is then a pure function of the raw vector and the
 * {@link Flavor} derived from the field's similarity function: independent of the other vectors in
 * the segment. Identical vectors thus quantize identically and the quantized data de-duplicates
 * along with the raw data, shared across all documents and fields of a group with the same flavor.
 *
 * <p>For the same reason, a record already computed by a source segment does not change on merge:
 * when a distinct vector originates from a segment in this format (with the same encoding and
 * flavor), its quantized record is <b>copied</b> instead of re-reading the raw vector and
 * re-quantizing it.
 *
 * <p>Records follow the {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat} conventions exactly
 * (per flavor), so they are scored by the stock {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer}.
 *
 * @lucene.experimental
 */
record DedupQuantizer(ScalarEncoding encoding) {

  /**
   * How a vector is prepared and quantized, derived from the field's {@link
   * VectorSimilarityFunction}. Fields of a group whose similarity functions map to the same flavor
   * share one quantized record per distinct vector; a group stores one block per flavor in use.
   */
  enum Flavor {

    /** Vectors quantized as-is; the additional correction holds the squared norm. */
    EUCLIDEAN(0, VectorSimilarityFunction.EUCLIDEAN, false),

    /**
     * Vectors quantized as-is; the additional correction holds the dot product with the (zero)
     * centroid, i.e. zero.
     */
    DOT_PRODUCT(1, VectorSimilarityFunction.DOT_PRODUCT, false),

    /**
     * Vectors are l2-normalized before quantization (for cosine similarity, scored as a dot
     * product); the additional correction is zero as for {@link #DOT_PRODUCT}.
     */
    NORMALIZED(2, VectorSimilarityFunction.DOT_PRODUCT, true);

    /** The number used to identify this flavor on the wire, rather than relying on ordinal. */
    private final int wireNumber;

    private final VectorSimilarityFunction quantizerFunction;
    private final boolean normalized;

    Flavor(int wireNumber, VectorSimilarityFunction quantizerFunction, boolean normalized) {
      this.wireNumber = wireNumber;
      this.quantizerFunction = quantizerFunction;
      this.normalized = normalized;
    }

    static Flavor of(VectorSimilarityFunction function) {
      return switch (function) {
        case EUCLIDEAN -> EUCLIDEAN;
        case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> DOT_PRODUCT;
        case COSINE -> NORMALIZED;
      };
    }

    int getWireNumber() {
      return wireNumber;
    }

    /** Returns the flavor for the given wire number, or empty if unknown. */
    static Optional<Flavor> fromWireNumber(int wireNumber) {
      for (Flavor flavor : values()) {
        if (flavor.wireNumber == wireNumber) {
          return Optional.of(flavor);
        }
      }
      return Optional.empty();
    }

    /**
     * The quantizer producing this flavor's records against a zero centroid. NOTE: {@link
     * #NORMALIZED} quantizes with {@link VectorSimilarityFunction#DOT_PRODUCT} (identical bytes and
     * corrections, without the unit-centroid requirement of a cosine-configured quantizer).
     */
    OptimizedScalarQuantizer quantizer() {
      return new OptimizedScalarQuantizer(quantizerFunction);
    }

    boolean normalized() {
      return normalized;
    }
  }

  /** Supplies the distinct (raw) float vector at a group ordinal. */
  interface FloatVectorSupplier {
    float[] get(int ord) throws IOException;
  }

  /**
   * The already-quantized record of a distinct vector, held by a source segment at {@code ord} with
   * the given flavor.
   */
  record PreQuantized(QuantizedByteVectorValues values, Flavor flavor, int ord) {}

  /**
   * Supplies the {@link PreQuantized} record of the distinct vector at a group ordinal, or {@code
   * null} when its source segment does not hold one.
   */
  interface PreQuantizedSupplier {
    PreQuantized get(int ord) throws IOException;
  }

  /** The encoding, offset and size of one flavor's quantized data block. */
  record QuantizedBlock(
      ScalarEncoding encoding, long quantizedDataOffset, long quantizedDataSize) {}

  /** Writes an empty flavor-block list, for groups without quantized data. */
  static void writeEmptyGroup(IndexOutput meta) throws IOException {
    meta.writeVInt(0);
  }

  /** Reads the flavor-block list of a group, the counterpart of {@link #writeGroup}. */
  static Map<Flavor, QuantizedBlock> readGroup(IndexInput meta) throws IOException {
    int numFlavors = meta.readVInt();
    Map<Flavor, QuantizedBlock> blocks = new EnumMap<>(Flavor.class);
    for (int i = 0; i < numFlavors; i++) {
      int flavorWireNumber = meta.readVInt();
      Flavor flavor =
          Flavor.fromWireNumber(flavorWireNumber)
              .orElseThrow(
                  () ->
                      new CorruptIndexException(
                          "Invalid flavor wire number: " + flavorWireNumber, meta));
      int wireNumber = meta.readVInt();
      ScalarEncoding encoding =
          ScalarEncoding.fromWireNumber(wireNumber)
              .orElseThrow(
                  () ->
                      new CorruptIndexException(
                          "Invalid scalar encoding wire number: " + wireNumber, meta));
      long offset = meta.readLong();
      long size = meta.readLong();
      if (blocks.put(flavor, new QuantizedBlock(encoding, offset, size)) != null) {
        throw new CorruptIndexException("Duplicate flavor: " + flavor, meta);
      }
    }
    return blocks;
  }

  /**
   * Quantizes and writes a group's distinct vectors: one block per flavor, one record per group
   * ordinal within each block. Wherever a {@code preQuantized} record with a matching encoding and
   * flavor is available (i.e. the vector originates from a segment in this format), it is copied
   * as-is; otherwise the raw vector is quantized. The block locations are written to {@code meta};
   * non-FLOAT32 groups (stored raw only) record an empty list.
   */
  void writeGroup(
      IndexOutput meta,
      IndexOutput quantizedVectorData,
      VectorEncoding groupEncoding,
      int dimension,
      int numVectors,
      Set<Flavor> flavors,
      FloatVectorSupplier vectors,
      PreQuantizedSupplier preQuantized)
      throws IOException {

    if (groupEncoding != VectorEncoding.FLOAT32) {
      writeEmptyGroup(meta);
      return;
    }

    meta.writeVInt(flavors.size());
    for (Flavor flavor : Flavor.values()) {
      if (flavors.contains(flavor) == false) {
        continue;
      }
      QuantizedBlock block =
          writeFlavorBlock(
              quantizedVectorData, flavor, dimension, numVectors, vectors, preQuantized);
      meta.writeVInt(flavor.getWireNumber());
      meta.writeVInt(block.encoding().getWireNumber());
      meta.writeLong(block.quantizedDataOffset());
      meta.writeLong(block.quantizedDataSize());
    }
  }

  private QuantizedBlock writeFlavorBlock(
      IndexOutput quantizedVectorData,
      Flavor flavor,
      int dimension,
      int numVectors,
      FloatVectorSupplier vectors,
      PreQuantizedSupplier preQuantized)
      throws IOException {

    OptimizedScalarQuantizer quantizer = flavor.quantizer();
    float[] zeroCentroid = new float[dimension];
    float[] normalized = flavor.normalized() ? new float[dimension] : null;
    byte[] scratch = new byte[encoding.getDiscreteDimensions(dimension)];
    byte[] packed =
        switch (encoding) {
          case UNSIGNED_BYTE, SEVEN_BIT -> scratch;
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };

    long quantizedDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    for (int ord = 0; ord < numVectors; ord++) {
      // The quantized record is a pure function of the raw vector and the flavor: a record
      // already computed by a source segment can be copied instead of re-quantizing.
      if (preQuantized != null) {
        PreQuantized pre = preQuantized.get(ord);
        if (pre != null && pre.flavor() == flavor && pre.values().getScalarEncoding() == encoding) {
          // NOTE: read the packed bytes before the corrective terms, which are then served from the
          // record cached by the read of the packed bytes
          writeRecord(
              quantizedVectorData,
              pre.values().vectorValue(pre.ord()),
              pre.values().getCorrectiveTerms(pre.ord()));
          continue;
        }
      }

      float[] vector = vectors.get(ord);
      if (flavor.normalized()) {
        // normalize a copy: the source buffer is shared / owned by the group
        System.arraycopy(vector, 0, normalized, 0, dimension);
        VectorUtil.l2normalize(normalized);
        vector = normalized;
      }
      // NOTE: scalarQuantize subtracts the centroid from the input in place, but with a zero
      // centroid the values are unchanged, so shared / owned buffers are safe to pass directly.
      OptimizedScalarQuantizer.QuantizationResult corrections =
          quantizer.scalarQuantize(vector, scratch, encoding.getBits(), zeroCentroid);
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, packed);
        case SINGLE_BIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.packAsBinary(scratch, packed);
        case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(scratch, packed);
        case UNSIGNED_BYTE, SEVEN_BIT -> {}
      }
      writeRecord(quantizedVectorData, packed, corrections);
    }
    long quantizedDataSize = quantizedVectorData.getFilePointer() - quantizedDataOffset;

    return new QuantizedBlock(encoding, quantizedDataOffset, quantizedDataSize);
  }

  private static void writeRecord(
      IndexOutput quantizedVectorData,
      byte[] packed,
      OptimizedScalarQuantizer.QuantizationResult corrections)
      throws IOException {
    quantizedVectorData.writeBytes(packed, packed.length);
    quantizedVectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
    quantizedVectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
    quantizedVectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
    quantizedVectorData.writeInt(corrections.quantizedComponentSum());
  }
}
