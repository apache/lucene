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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;

/**
 * The coarse tier: what a coarse code is, and how codes are compared.
 *
 * <p>A coarse tier defines three encodings:
 *
 * <ul>
 *   <li>The <b>document code</b>, {@link #docBits} bits per dimension (1 or 2), stored once per
 *       slot with no per-vector metadata. A cell's codes are a region of {@link #blockRows}-row
 *       blocks ({@link #regionBytes}). Document codes are relative to an anchor, the centroid of
 *       the slot's cell, and a tier keeps {@link #parameters} floats per field.
 *   <li>The <b>centroid code</b>, {@link #centroidBytes} bytes, which centroids are stored in and
 *       vectors are routed with: the centroid graph, clustering's routing and the graph's
 *       construction all compare centroid codes.
 *   <li>The <b>prepared query</b>, {@link #queryBytes} bytes, computed once per search: its first
 *       {@link #centroidBytes} bytes are the query's centroid code, and the rest is whatever the
 *       tier scores documents with.
 * </ul>
 *
 * <p>Every comparison returns an integer distance in {@code [0, bins)}, lower is nearer. Document
 * distances estimate the same similarity whatever the anchor, so they compare across cells and
 * segments. Unlike the fine tier, a coarse tier depends on data, through the anchor: coarse codes
 * are re-derived from the fine codes whenever a segment is written, so they never need to survive a
 * merge.
 */
abstract sealed class CoarseCodec permits CoarseCodec.S1 {
  final int dim;

  /** Bits per dimension of a document code: 1 or 2. */
  final int docBits;

  /** Bytes of a document code. */
  final int codeBytes;

  /** Bytes of a centroid code. */
  final int centroidBytes;

  /** Bytes of a prepared query. */
  final int queryBytes;

  /** Distances are in {@code [0, bins)}, so a histogram over them has {@code bins} buckets. */
  final int bins;

  CoarseCodec(int dim, int docBits, int codeBytes, int centroidBytes, int queryBytes, int bins) {
    // Coarse codes only allow 1- or 2-bit quantization for documents.
    if ((docBits == 1 || docBits == 2) == false) {
      throw new IllegalArgumentException("document codes take 1 or 2 bits; got " + docBits);
    }
    this.dim = dim;
    this.docBits = docBits;
    this.codeBytes = codeBytes;
    this.centroidBytes = centroidBytes;
    this.queryBytes = queryBytes;
    this.bins = bins;
  }

  static CoarseCodec of(CoarseTier tier, int dim) {
    return switch (tier) {
      case S1 -> new S1(dim);
    };
  }

  /**
   * How many floats the tier keeps per field, each the mean over every slot of a share that {@link
   * #encode} reports. These replace per-vector metadata.
   */
  abstract int parameters();

  // ---- Document codes.

  /** Documents per block of a cell's region: 1 when codes are stored one after another. */
  abstract int blockRows();

  /** Bytes of the region holding {@code rows} document codes: whole blocks. */
  final long regionBytes(int rows) {
    int blocks = (rows + blockRows() - 1) / blockRows();
    return (long) blocks * blockRows() * codeBytes;
  }

  /**
   * Encodes {@code vector} relative to {@code anchor}, its cell's centroid, into {@link #codeBytes}
   * at {@code offset}, and writes its {@link #parameters} shares from {@code sharesOffset}.
   */
  abstract void encode(
      float[] vector, float[] anchor, byte[] dest, int offset, float[] shares, int sharesOffset);

  /**
   * Lays out {@code rows} codes, consecutive in {@code codes}, as one block of {@link #blockRows}
   * rows in {@code block}; missing rows are zeros.
   */
  abstract void writeBlock(byte[] codes, int rows, byte[] block);

  // ---- Queries and centroid codes.

  /** Cells clustering keeps from a scan of the centroid codes before ranking them exactly. */
  abstract int routingShortlist();

  /** Encodes {@code vector}, a centroid or a vector being routed, into {@link #centroidBytes}. */
  abstract void encodeCentroid(float[] vector, byte[] dest, int offset);

  /**
   * Prepares the coarse cell graph and query vector quantization codes. Coarse cell routing cannot
   * have cell-anchored quantization codes by definition, but document codes are.
   */
  abstract void encodeQuery(float[] vector, byte[] dest, int offset);

  /**
   * As {@link #encodeQuery(float[], byte[], int)}, reusing {@code levels}, the INT8 levels of the
   * same query, where this tier builds on them.
   */
  void encodeQuery(float[] vector, QueryLevels levels, byte[] dest, int offset) {
    encodeQuery(vector, dest, offset);
  }

  /**
   * Adjusts {@code prepared}, a prepared query, to codes relative to a cell's centroid {@code a},
   * given the query's exact {@code queryAnchorDot = q.a} and the field's {@code parameters}.
   */
  abstract void anchorQuery(byte[] prepared, float queryAnchorDot, float[] parameters);

  // ---- Query against document codes.

  /** Distances to the {@code rows} document codes of the region at {@code offset}. */
  abstract void distances(byte[] query, MemorySegment codes, long offset, int rows, int[] out);

  /** Distance to document code {@code row} of the region at {@code offset}. */
  abstract int distance(byte[] query, MemorySegment codes, long offset, int row);

  // ---- Query against centroid codes.

  /** Distance to the centroid code at {@code offset}. */
  int centroidDistance(byte[] query, byte[] centroids, int offset) {
    return Kernels.INSTANCE.hamming(query, centroidBytes, centroids, offset);
  }

  /** Distances to {@code rows} consecutive centroid codes starting at {@code offset}. */
  void centroidDistances(byte[] query, byte[] centroids, int offset, int rows, int[] out) {
    for (int r = 0; r < rows; r++) {
      out[r] = centroidDistance(query, centroids, offset + r * centroidBytes);
    }
  }

  /**
   * Distances to the centroid codes starting at each of the first {@code count} offsets. A heap
   * {@code centroids} must wrap a whole {@code byte[]}; a native one is only supported by tiers
   * whose centroid codes are compared by Hamming distance (see {@link #nativeCentroidCodes}).
   */
  final void centroidDistancesAt(
      byte[] query, MemorySegment centroids, int[] offsets, int count, int[] out) {
    if (centroids.isNative()) {
      assert nativeCentroidCodes() : getClass().getSimpleName();
      for (int i = 0; i < count; i++) {
        out[i] = Kernels.INSTANCE.hamming(query, centroidBytes, centroids, offsets[i]);
      }
    } else {
      byte[] array = (byte[]) centroids.heapBase().orElseThrow();
      for (int i = 0; i < count; i++) {
        out[i] = centroidDistance(query, array, offsets[i]);
      }
    }
  }

  /**
   * Whether centroid codes may live in native memory: true when {@link #centroidDistance} is the
   * Hamming distance over {@link #centroidBytes}, which {@link #centroidDistancesAt} then computes
   * straight from the segment.
   */
  boolean nativeCentroidCodes() {
    return false;
  }

  /**
   * S1: binary codes against the cell's centroid, scored by table lookup: one bit per dimension,
   * the sign of a document's offset from the centroid {@code a} of the cell it is stored in,
   * against a table of the query per 4 dimensions. Codes carry no per-vector metadata, and the
   * field keeps one float.
   *
   * <p><b>A document</b> is {@code 4 * ceil(dim / 32)} bytes of bits {@code b_i = [x_i > a_i]},
   * nothing else, stored in blocks of {@link Kernels#BLOCK_ROWS} documents so a scan looks up many
   * documents' 4-dimension groups at once ({@link Kernels#tableBins}). The code is written once the
   * centroids are final, so it is relative to the cell it is scanned in, and a document is
   * estimated as {@code a + u (2b - 1)}:
   *
   * <pre>
   *   q.x ~ q.a + u * q.(2b - 1)
   * </pre>
   *
   * <p>{@code u = |x - a|^2 / sum |x_i - a_i|} is the unbiased scale of a sign code, as in RaBitQ,
   * and the field keeps its mean over every slot. Nothing else is needed along the centroid:
   * vectors and centroids are unit length, so {@code a.x = 1 - |x - a|^2 / 2}, and for a rotated
   * offset the signs' own part along {@code a} is, in expectation, exactly that. Per-cell or
   * per-document means of {@code a.x}, {@code u} or {@code a.(2b - 1)} gained no recall over one
   * mean.
   *
   * <p><b>A query</b> is quantized once, to the fine tier's INT8 code {@code l} of it, and becomes
   * one table per 4-dimension group of {@code l.(2b - 1)} over the group's 16 sign patterns, in
   * 5-bit entries: {@code q.(2b - 1) ~ delta * U - O}, where {@code U} is the sum of a document's
   * entries. The tables depend only on the query, so they are built once per search; anchoring to a
   * cell sets {@code q.a}, so a document costs one table sum and one multiply-add.
   *
   * <p><b>Centroids</b> are the sign bits of the centroid itself, compared by Hamming distance with
   * the query's sign bits, which lead the prepared query: cheap proposals that the search and
   * clustering rerank exactly against the float centroids.
   */
  static final class S1 extends CoarseCodec {
    /** Bins per unit of dot product. */
    static final int SCALE = 1 << 11;

    /** The largest table entry: eight, four code bytes' lookups, add in a byte. */
    static final int MAX_ENTRY = 31;

    // After the sign bits and the tables: the table scale and offset, then the anchored base and
    // scale.
    private static final int QUERY_DELTA = 0;
    private static final int QUERY_OFFSET = 4;
    private static final int QUERY_BASE = 8;
    private static final int QUERY_SCALE = 12;
    private static final int QUERY_FLOATS = 4 * Float.BYTES;

    /**
     * Bytes of a document code, and of the sign bits leading a query: whole 4-byte words, so the
     * scan looks up four code bytes per widening ({@link Kernels#tableBins}).
     */
    private final int planeBytes;

    /** Where the tables start in a prepared query: after its sign bits. */
    private final int tablesOffset;

    /** Where the floats start: after the tables. */
    private final int floatsOffset;

    S1(int dim) {
      super(
          dim,
          1,
          planeBytes(dim),
          (dim + 7) >>> 3,
          floatsOffset(planeBytes(dim)) + QUERY_FLOATS,
          Math.max(2 * SCALE + 2, 8 * ((dim + 7) >>> 3) + 1));
      planeBytes = codeBytes;
      tablesOffset = planeBytes;
      floatsOffset = floatsOffset(planeBytes);
    }

    private static int planeBytes(int dim) {
      return 4 * ((dim + 31) >>> 5);
    }

    private static int floatsOffset(int planeBytes) {
      return planeBytes + Kernels.TABLE_BYTES * planeBytes;
    }

    @Override
    int parameters() {
      return 1;
    }

    @Override
    int blockRows() {
      return Kernels.BLOCK_ROWS;
    }

    /** Writes the sign bits of {@code vector - anchor}, and its share {@code u}. */
    @Override
    void encode(
        float[] vector, float[] anchor, byte[] dest, int offset, float[] shares, int sharesOffset) {
      Arrays.fill(dest, offset, offset + planeBytes, (byte) 0);
      double absoluteSum = 0;
      double squareSum = 0;
      for (int d = 0; d < dim; d++) {
        float delta = vector[d] - anchor[d];
        absoluteSum += Math.abs(delta);
        squareSum += delta * delta;
        if (delta > 0) {
          dest[offset + (d >>> 3)] |= (byte) (1 << (d & 7));
        }
      }
      shares[sharesOffset] = absoluteSum == 0 ? 0f : (float) (squareSum / absoluteSum);
    }

    @Override
    void writeBlock(byte[] codes, int rows, byte[] block) {
      Arrays.fill(block, 0, Kernels.BLOCK_ROWS * planeBytes, (byte) 0);
      for (int r = 0; r < rows; r++) {
        for (int j = 0; j < planeBytes; j++) {
          block[j * Kernels.BLOCK_ROWS + Kernels.blockPosition(r)] = codes[r * planeBytes + j];
        }
      }
    }

    /**
     * Sign bits rank centroids coarsely, so routing ranks 16 of them exactly: 32 gained no
     * measurable recall over 16 and built 20-25% slower.
     */
    @Override
    int routingShortlist() {
      return 16;
    }

    /** Centroid codes are sign bits compared by Hamming distance, so the graph aligns them. */
    @Override
    boolean nativeCentroidCodes() {
      return true;
    }

    @Override
    void encodeCentroid(float[] vector, byte[] dest, int offset) {
      Arrays.fill(dest, offset, offset + centroidBytes, (byte) 0);
      for (int d = 0; d < dim; d++) {
        if (vector[d] > 0) {
          dest[offset + (d >>> 3)] |= (byte) (1 << (d & 7));
        }
      }
    }

    /**
     * Prepares a query from the fine tier's INT8 code of it ({@link FineCodec.Int8#queryLevels}):
     * its sign bits, then per 4-dimension group the 16 sums {@code l.(2b - 1)} of its levels {@code
     * l}, offset by {@code sum(|l|)} to be non-negative and divided by 32 to fit {@link
     * #MAX_ENTRY}, all in integers.
     */
    @Override
    void encodeQuery(float[] vector, byte[] dest, int offset) {
      byte[] levels = new byte[8 * planeBytes];
      encodeLevels(levels, FineCodec.Int8.queryLevels(vector, dim, levels), dest, offset);
    }

    @Override
    void encodeQuery(float[] vector, QueryLevels levels, byte[] dest, int offset) {
      // The tables cover whole 32-dimension words; the padding's levels are zero.
      byte[] padded = ArrayUtil.growExact(levels.levels(), 8 * planeBytes);
      encodeLevels(padded, levels.unitScale(), dest, offset);
    }

    /** Writes the prepared query of {@code levels}, {@code 8 * planeBytes} of them, at scale. */
    private void encodeLevels(byte[] levels, float levelScale, byte[] dest, int offset) {
      Arrays.fill(dest, offset, offset + floatsOffset, (byte) 0);
      for (int d = 0; d < dim; d++) {
        if (levels[d] > 0) {
          dest[offset + (d >>> 3)] |= (byte) (1 << (d & 7));
        }
      }
      // One table unit is 32 INT8 units: q.(2b - 1) ~ levelScale * (32 * U - sum(|l|)).
      int groups = 2 * planeBytes;
      long absoluteSum = 0;
      for (int g = 0; g < groups; g++) {
        int l0 = levels[4 * g];
        int l1 = levels[4 * g + 1];
        int l2 = levels[4 * g + 2];
        int l3 = levels[4 * g + 3];
        int groupAbsolute = Math.abs(l0) + Math.abs(l1) + Math.abs(l2) + Math.abs(l3);
        absoluteSum += groupAbsolute;
        // Byte j's low nibble is group 2j, its high nibble group 2j + 1; each table stored twice.
        int table =
            offset
                + tablesOffset
                + Kernels.TABLE_BYTES * (g / 2)
                + (g % 2 == 0 ? 0 : Kernels.HIGH_TABLE);
        for (int pattern = 0; pattern < 16; pattern++) {
          int sum =
              ((pattern & 1) != 0 ? l0 : -l0)
                  + ((pattern & 2) != 0 ? l1 : -l1)
                  + ((pattern & 4) != 0 ? l2 : -l2)
                  + ((pattern & 8) != 0 ? l3 : -l3);
          // In [0, 2 * 4 * 127], rounded to the nearest 32nd and capped at MAX_ENTRY.
          dest[table + pattern] = (byte) Math.min(MAX_ENTRY, (sum + groupAbsolute + 16) >> 5);
        }
        System.arraycopy(dest, table, dest, table + 16, 16);
      }
      putFloat(dest, offset + floatsOffset + QUERY_DELTA, 32 * levelScale);
      putFloat(dest, offset + floatsOffset + QUERY_OFFSET, levelScale * absoluteSum);
      putFloat(dest, offset + floatsOffset + QUERY_BASE, 0f);
      putFloat(dest, offset + floatsOffset + QUERY_SCALE, 0f);
    }

    @Override
    void anchorQuery(byte[] prepared, float queryAnchorDot, float[] parameters) {
      putFloat(prepared, floatsOffset + QUERY_BASE, queryAnchorDot);
      putFloat(prepared, floatsOffset + QUERY_SCALE, parameters[0]);
    }

    @Override
    void distances(byte[] query, MemorySegment codes, long offset, int rows, int[] out) {
      Kernels.INSTANCE.tableBins(
          query,
          tablesOffset,
          planeBytes,
          codes,
          offset,
          rows,
          binBase(query),
          binSlope(query),
          MAX_BIN,
          out);
    }

    @Override
    int distance(byte[] query, MemorySegment codes, long offset, int row) {
      int sum = Kernels.INSTANCE.tableSum(query, tablesOffset, planeBytes, codes, offset, row);
      return Kernels.tableBin(sum, binBase(query), binSlope(query), MAX_BIN);
    }

    // q.x ~ base + scale * (delta * U - O): every term but U is fixed per query and cell, so a
    // document's bin, of 1 - q.x, is one affine map of U and a clamp.
    private static final int MAX_BIN = 2 * SCALE;

    private float binBase(byte[] query) {
      float scale = getFloat(query, floatsOffset + QUERY_SCALE);
      float base =
          getFloat(query, floatsOffset + QUERY_BASE)
              - scale * getFloat(query, floatsOffset + QUERY_OFFSET);
      return (1f - base) * SCALE + 0.5f;
    }

    private float binSlope(byte[] query) {
      float scale = getFloat(query, floatsOffset + QUERY_SCALE);
      return -scale * getFloat(query, floatsOffset + QUERY_DELTA) * SCALE;
    }

    private static float getFloat(byte[] bytes, int offset) {
      return Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(bytes, offset));
    }

    private static void putFloat(byte[] bytes, int offset, float value) {
      BitUtil.VH_LE_INT.set(bytes, offset, Float.floatToIntBits(value));
    }
  }
}
