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

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.BIT_COUNT;
import static jdk.incubator.vector.VectorOperators.F2I;
import static jdk.incubator.vector.VectorOperators.I2F;
import static jdk.incubator.vector.VectorOperators.LSHL;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.S2I;
import static jdk.incubator.vector.VectorOperators.XOR;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Optional;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;

/**
 * The inner loops of SegmentIVF: Hamming distances for centroid sign codes, table lookups for S1's
 * 1-bit codes, coarse admission, and INT8 dot products for the fine rerank.
 *
 * <p>This class holds the scalar reference implementations. {@link #INSTANCE} is the {@link Panama}
 * subclass when the {@code jdk.incubator.vector} module is available, which overrides each method
 * with SIMD code and falls back to the scalar code for shapes it does not handle. Codes are read
 * either from heap arrays or in place from mapped or off-heap memory.
 */
class Kernels {
  static final Kernels INSTANCE = load();

  // Core: VectorizationProvider does this detection, but internal.vectorization isn't exported.
  private static Kernels load() {
    // As core's checks: on another VM, or without C2, the Vector API runs its boxed Java fallback,
    // many times slower than the scalar kernels. Lucene's tests run C1 only but set
    // tests.vectorsize, and test the SIMD kernels anyway, as core's tests do. The table kernels
    // also need JDK 24's selectFrom, which wraps out-of-range indexes (earlier JDKs throw); this
    // module targets 24.
    Optional<Module> vectorModule = ModuleLayer.boot().findModule("jdk.incubator.vector");
    boolean simd =
        vectorModule.isPresent()
            && Constants.IS_HOTSPOT_VM
            && (Constants.IS_CLIENT_VM == false || System.getProperty("tests.vectorsize") != null);
    if (simd == false) {
      return new Kernels();
    }
    try {
      // The module descriptor deliberately has no `requires jdk.incubator.vector` (see
      // module-info.java), so add the read edge before Panama links against the Vector API, as
      // core's VectorizationProvider does.
      Kernels.class.getModule().addReads(vectorModule.get());
      Class<?> panama = Class.forName(Kernels.class.getName() + "$Panama");
      return (Kernels) panama.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException | LinkageError _) {
      // Includes a Panama constructor refusing vectors narrower than 128 bits.
      return new Kernels();
    }
  }

  // ---- Hamming distance: popcount(query XOR code), for centroid sign codes.
  // Core: VectorUtil#xorBitCount, but scalar and whole-array only (no offset/segment/SIMD).

  /** Hamming distance between the first {@code length} bytes of {@code query} and a code. */
  int hamming(byte[] query, int length, byte[] codes, int offset) {
    int sum = 0;
    for (int i = 0; i < length; i++) {
      sum += Integer.bitCount((query[i] ^ codes[offset + i]) & 0xFF);
    }
    return sum;
  }

  /**
   * Hamming distance between the first {@code length} bytes of {@code query} and the code at {@code
   * offset}.
   */
  int hamming(byte[] query, int length, MemorySegment codes, long offset) {
    return scalarHamming(query, length, codes, offset, 0);
  }

  /** Hamming distance over bytes {@code [from, length)} of the code at {@code offset}. */
  static int scalarHamming(byte[] query, int length, MemorySegment codes, long offset, int from) {
    int sum = 0;
    for (int i = from; i < length; i++) {
      byte code = codes.get(ValueLayout.JAVA_BYTE, offset + i);
      sum += Integer.bitCount((query[i] ^ code) & 0xFF);
    }
    return sum;
  }

  // ---- Table sums, for S1's 1-bit codes in blocks.
  //
  // A cell's codes are stored in blocks of BLOCK_ROWS documents: byte j of every document of a
  // block, 8 dimensions, sits at j * BLOCK_ROWS + blockPosition(row), so one vector load holds the
  // same 8 dimensions of many documents. A byte is two 4-dimension groups, its low and high nibble;
  // the query is a 16-entry table per group, TABLE_BYTES per byte j: the low nibble's table twice,
  // then the high nibble's twice. A document's sum is the sum over its groups of the entry its
  // nibble selects: one table lookup (TBL) per 4 dimensions, the cheapest exact use of a multi-bit
  // query against 1-bit codes. Entries are at most 31, so four code bytes' eight lookups add in a
  // byte.
  //
  // Each table is stored twice so the whole code byte can index the low one: a lookup wraps its
  // index to the vector's 16 or 32 lanes, and either way lands on entry (byte & 15), so the scan
  // needs no mask for the low nibble. Doubling the high one too makes a code byte's tables a whole
  // number of vectors, so their loads need no address arithmetic.
  //
  // A block is 128 rows so that one load of a code byte's tables serves four 32-lane vectors of
  // documents, eight of 16 lanes. Within each 32 rows of a block, rows are interleaved so that,
  // read as shorts, the low bytes are rows 0-15 and the high bytes rows 16-31: the scan widens its
  // byte sums in pairs, and each half then comes out as consecutive rows.

  /** Documents per block of table-scored codes. */
  static final int BLOCK_ROWS = 128;

  /** Rows interleaved together within a block: the widest vector of bytes the scan uses. */
  private static final int INTERLEAVED_ROWS = 32;

  /** Table bytes per code byte: the low nibble's table twice, then the high nibble's twice. */
  static final int TABLE_BYTES = 64;

  /** Where a code byte's high-nibble table starts within its {@link #TABLE_BYTES}. */
  static final int HIGH_TABLE = 32;

  /**
   * The bins of the table sums of {@code rows} documents stored in blocks from {@code offset}, each
   * block {@code bytes * BLOCK_ROWS} bytes, with the tables of {@code bytes} code bytes from {@code
   * tablesOffset}: {@link #tableBin} of each sum.
   */
  void tableBins(
      byte[] tables,
      int tablesOffset,
      int bytes,
      MemorySegment codes,
      long offset,
      int rows,
      float binBase,
      float binSlope,
      int maxBin,
      int[] out) {
    for (int r = 0; r < rows; r++) {
      int sum = tableSum(tables, tablesOffset, bytes, codes, offset, r);
      out[r] = tableBin(sum, binBase, binSlope, maxBin);
    }
  }

  /** The bin of a table sum: {@code binBase + binSlope * sum}, truncated and clamped. */
  static int tableBin(int sum, float binBase, float binSlope, int maxBin) {
    return Math.max(0, Math.min(maxBin, (int) (binBase + binSlope * sum)));
  }

  /** Where row {@code row} of a block sits within each of its {@link #BLOCK_ROWS}-byte runs. */
  static int blockPosition(int row) {
    int r = row % BLOCK_ROWS;
    int half = INTERLEAVED_ROWS / 2;
    return r - r % INTERLEAVED_ROWS + 2 * (r % half) + r % INTERLEAVED_ROWS / half;
  }

  /** The table sum of document {@code row} of the blocks from {@code offset}. */
  final int tableSum(
      byte[] tables, int tablesOffset, int bytes, MemorySegment codes, long offset, int row) {
    long block = offset + (long) (row / BLOCK_ROWS) * bytes * BLOCK_ROWS + blockPosition(row);
    int sum = 0;
    for (int j = 0; j < bytes; j++) {
      int code = codes.get(ValueLayout.JAVA_BYTE, block + (long) j * BLOCK_ROWS) & 0xFF;
      int table = tablesOffset + TABLE_BYTES * j;
      sum += tables[table + (code & 15)] + tables[table + HIGH_TABLE + (code >>> 4)];
    }
    return sum;
  }

  // ---- Coarse admission.
  // Core: VectorUtil#filterByScore, but for double scores in place, not int distances to indexes.

  /**
   * Writes the indexes, relative to {@code from}, of the {@code count} distances starting at {@code
   * from} that are at most {@code threshold} into {@code out}, and returns how many there are.
   */
  int filterAtMost(int[] distances, int from, int count, int threshold, int[] out) {
    int kept = 0;
    for (int i = 0; i < count; i++) {
      if (distances[from + i] <= threshold) {
        out[kept++] = i;
      }
    }
    return kept;
  }

  // ---- INT8 dot products, for the fine rerank.
  // Core: PanamaVectorUtilSupport#dotProduct(byte[], MemorySegment), internal to lucene-core.

  /** Dot product of {@code query} with the code at {@code offset} of {@code records}. */
  int dotProduct(byte[] query, byte[] records, int offset) {
    int sum = 0;
    for (int i = 0; i < query.length; i++) {
      sum += query[i] * records[offset + i];
    }
    return sum;
  }

  /** Dot products of {@code query} with {@code count} codes {@code stride} bytes apart. */
  void dotProducts(byte[] query, byte[] records, int stride, int count, int[] out) {
    for (int r = 0; r < count; r++) {
      out[r] = dotProduct(query, records, r * stride);
    }
  }

  /** Dot products of {@code query} with the codes starting at {@code offsets}, read in place. */
  void dotProducts(byte[] query, MemorySegment records, long[] offsets, int count, int[] out) {
    for (int r = 0; r < count; r++) {
      out[r] = scalarDot(query, records, offsets[r], 0);
    }
  }

  /** Dot product over bytes {@code [from, query.length)} of the code at {@code offset}. */
  static int scalarDot(byte[] query, MemorySegment records, long offset, int from) {
    int sum = 0;
    for (int i = from; i < query.length; i++) {
      sum += query[i] * records.get(ValueLayout.JAVA_BYTE, offset + i);
    }
    return sum;
  }

  /**
   * SIMD implementations.
   *
   * <p>Hamming kernels process {@link #BYTES} bytes per step as longs. Table sums look up a block's
   * 4-dimension groups in byte lanes, {@link #BYTES} documents per load. INT8 dot products widen 8
   * bytes at a time to 256-bit integer lanes, and fall back to {@link VectorUtil#dotProduct(byte[],
   * byte[])} where the preferred integer species is not 256 bits: narrower, as NEON's 128, or
   * wider, as AVX-512's, whose kernels VectorUtil already has.
   *
   * <p>No vector value is passed to or returned from a method of this class, so there are no
   * helpers for loads or popcounts: C2 inlines these kernels into large callers such as the
   * centroid graph walk, and once it hits its inlining budget it stops inlining small helpers too.
   * A vector crossing a call that is not inlined is boxed, and every operation on it then runs the
   * Vector API's scalar fallback, several times slower.
   *
   * <p>C2 can also box these kernels' vectors when it compiles them from a thin profile, as when
   * many threads run them cold at once; a single-threaded warmup avoids it ({@link
   * SegmentIVFVectorsFormat}).
   */
  static final class Panama extends Kernels {
    private static final VectorSpecies<Long> LONGS = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Byte> BYTES = LONGS.withLanes(byte.class);
    private static final VectorSpecies<Short> SHORTS = LONGS.withLanes(short.class);
    private static final VectorSpecies<Integer> INTS = LONGS.withLanes(int.class);

    /**
     * The order code longs are loaded in. A query's longs come from {@code reinterpretAsLongs},
     * which the Vector API defines as little-endian on every platform, so codes must load
     * little-endian too for their bytes to pair with the query's; native order would pair byte
     * {@code i} with byte {@code 7 - i} on a big-endian platform.
     */
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final VectorSpecies<Byte> DOT_BYTES = ByteVector.SPECIES_64;
    private static final VectorSpecies<Integer> DOT_INTS = IntVector.SPECIES_256;
    private static final boolean DOT_IS_256 =
        IntVector.SPECIES_PREFERRED.vectorBitSize() == DOT_INTS.vectorBitSize();

    /** A code copied for the {@link VectorUtil} fallback, reused rather than one per call. */
    private static final ThreadLocal<byte[][]> FALLBACK_CODE =
        ThreadLocal.withInitial(() -> new byte[][] {new byte[0]});

    Panama() {
      if (LONGS.vectorBitSize() < 128) {
        throw new UnsupportedOperationException("preferred vectors of " + LONGS.vectorBitSize());
      }
    }

    /** The thread's fallback copy buffer, of exactly {@code length} bytes as VectorUtil needs. */
    private static byte[] fallbackCode(int length) {
      byte[][] holder = FALLBACK_CODE.get();
      if (holder[0].length != length) {
        holder[0] = new byte[length];
      }
      return holder[0];
    }

    // ---- Hamming.

    @Override
    int hamming(byte[] query, int length, MemorySegment codes, long offset) {
      if (codes.isNative() == false) {
        return scalarHamming(query, length, codes, offset, 0);
      }
      LongVector sum = LongVector.zero(LONGS);
      int i = 0;
      for (; i <= length - BYTES.length(); i += BYTES.length()) {
        sum =
            sum.add(
                ByteVector.fromArray(BYTES, query, i)
                    .reinterpretAsLongs()
                    .lanewise(XOR, LongVector.fromMemorySegment(LONGS, codes, offset + i, ORDER))
                    .lanewise(BIT_COUNT));
      }
      return (int) sum.reduceLanes(ADD) + scalarHamming(query, length, codes, offset, i);
    }

    @Override
    int hamming(byte[] query, int length, byte[] codes, int offset) {
      LongVector sum = LongVector.zero(LONGS);
      int i = 0;
      for (; i <= length - BYTES.length(); i += BYTES.length()) {
        sum =
            sum.add(
                ByteVector.fromArray(BYTES, query, i)
                    .reinterpretAsLongs()
                    .lanewise(
                        XOR, ByteVector.fromArray(BYTES, codes, offset + i).reinterpretAsLongs())
                    .lanewise(BIT_COUNT));
      }
      int total = (int) sum.reduceLanes(ADD);
      for (; i < length; i++) {
        total += Integer.bitCount((query[i] ^ codes[offset + i]) & 0xFF);
      }
      return total;
    }

    // ---- Table sums.

    /** Code bytes whose eight lookups, at most 31 each, add in a byte before widening. */
    private static final int BYTES_PER_WIDENING = 4;

    /**
     * The most code bytes a scan sums in shorts: each adds at most 62 to a document's sum, which
     * must stay below 2^15 to widen to an int as a signed short.
     */
    private static final int MAX_TABLE_BYTES = 512;

    /** Vectors of documents scanned together, so that each table load serves this many. */
    private static final int GROUP = 4;

    @Override
    void tableBins(
        byte[] tables,
        int tablesOffset,
        int bytes,
        MemorySegment codes,
        long offset,
        int rows,
        float binBase,
        float binSlope,
        int maxBin,
        int[] out) {
      int lanes = BYTES.length();
      if ((lanes != 16 && lanes != 32)
          || bytes % BYTES_PER_WIDENING != 0
          || bytes > MAX_TABLE_BYTES
          || tables.length < tablesOffset + TABLE_BYTES * bytes) {
        super.tableBins(
            tables, tablesOffset, bytes, codes, offset, rows, binBase, binSlope, maxBin, out);
        return;
      }
      long blockBytes = (long) bytes * BLOCK_ROWS;
      int shorts = SHORTS.length();
      short[] sums = new short[2 * GROUP * shorts];
      for (int first = 0; first < rows; first += BLOCK_ROWS) {
        long block = offset + (first / BLOCK_ROWS) * blockBytes;
        for (int group = 0; group < BLOCK_ROWS; group += GROUP * lanes) {
          long at = block + group;
          // Each short lane holds two documents' sums in its low and high byte. Summing whole
          // shorts carries the low sums into the high ones, so the high sums are kept apart too,
          // and the low ones recovered at the end: all = low + 256 * high, modulo 2^16.
          ShortVector all0 = ShortVector.zero(SHORTS);
          ShortVector all1 = ShortVector.zero(SHORTS);
          ShortVector all2 = ShortVector.zero(SHORTS);
          ShortVector all3 = ShortVector.zero(SHORTS);
          ShortVector high0 = ShortVector.zero(SHORTS);
          ShortVector high1 = ShortVector.zero(SHORTS);
          ShortVector high2 = ShortVector.zero(SHORTS);
          ShortVector high3 = ShortVector.zero(SHORTS);
          for (int j = 0; j < bytes; j += BYTES_PER_WIDENING) {
            // Four code bytes, written out: C2 does not unroll an inner loop this large, and a
            // loop here would also keep a safepoint and the code loads' bounds checks in the scan.
            int table = tablesOffset + TABLE_BYTES * j;
            long run = at + (long) j * BLOCK_ROWS;
            ByteVector low = ByteVector.fromArray(BYTES, tables, table);
            ByteVector high = ByteVector.fromArray(BYTES, tables, table + HIGH_TABLE);
            ByteVector code0 = ByteVector.fromMemorySegment(BYTES, codes, run, ORDER);
            ByteVector code1 = ByteVector.fromMemorySegment(BYTES, codes, run + lanes, ORDER);
            ByteVector code2 = ByteVector.fromMemorySegment(BYTES, codes, run + 2L * lanes, ORDER);
            ByteVector code3 = ByteVector.fromMemorySegment(BYTES, codes, run + 3L * lanes, ORDER);
            ByteVector sum0 = code0.selectFrom(low).add(code0.lanewise(LSHR, 4).selectFrom(high));
            ByteVector sum1 = code1.selectFrom(low).add(code1.lanewise(LSHR, 4).selectFrom(high));
            ByteVector sum2 = code2.selectFrom(low).add(code2.lanewise(LSHR, 4).selectFrom(high));
            ByteVector sum3 = code3.selectFrom(low).add(code3.lanewise(LSHR, 4).selectFrom(high));
            table += TABLE_BYTES;
            run += BLOCK_ROWS;
            low = ByteVector.fromArray(BYTES, tables, table);
            high = ByteVector.fromArray(BYTES, tables, table + HIGH_TABLE);
            code0 = ByteVector.fromMemorySegment(BYTES, codes, run, ORDER);
            code1 = ByteVector.fromMemorySegment(BYTES, codes, run + lanes, ORDER);
            code2 = ByteVector.fromMemorySegment(BYTES, codes, run + 2L * lanes, ORDER);
            code3 = ByteVector.fromMemorySegment(BYTES, codes, run + 3L * lanes, ORDER);
            sum0 = sum0.add(code0.selectFrom(low)).add(code0.lanewise(LSHR, 4).selectFrom(high));
            sum1 = sum1.add(code1.selectFrom(low)).add(code1.lanewise(LSHR, 4).selectFrom(high));
            sum2 = sum2.add(code2.selectFrom(low)).add(code2.lanewise(LSHR, 4).selectFrom(high));
            sum3 = sum3.add(code3.selectFrom(low)).add(code3.lanewise(LSHR, 4).selectFrom(high));
            table += TABLE_BYTES;
            run += BLOCK_ROWS;
            low = ByteVector.fromArray(BYTES, tables, table);
            high = ByteVector.fromArray(BYTES, tables, table + HIGH_TABLE);
            code0 = ByteVector.fromMemorySegment(BYTES, codes, run, ORDER);
            code1 = ByteVector.fromMemorySegment(BYTES, codes, run + lanes, ORDER);
            code2 = ByteVector.fromMemorySegment(BYTES, codes, run + 2L * lanes, ORDER);
            code3 = ByteVector.fromMemorySegment(BYTES, codes, run + 3L * lanes, ORDER);
            sum0 = sum0.add(code0.selectFrom(low)).add(code0.lanewise(LSHR, 4).selectFrom(high));
            sum1 = sum1.add(code1.selectFrom(low)).add(code1.lanewise(LSHR, 4).selectFrom(high));
            sum2 = sum2.add(code2.selectFrom(low)).add(code2.lanewise(LSHR, 4).selectFrom(high));
            sum3 = sum3.add(code3.selectFrom(low)).add(code3.lanewise(LSHR, 4).selectFrom(high));
            table += TABLE_BYTES;
            run += BLOCK_ROWS;
            low = ByteVector.fromArray(BYTES, tables, table);
            high = ByteVector.fromArray(BYTES, tables, table + HIGH_TABLE);
            code0 = ByteVector.fromMemorySegment(BYTES, codes, run, ORDER);
            code1 = ByteVector.fromMemorySegment(BYTES, codes, run + lanes, ORDER);
            code2 = ByteVector.fromMemorySegment(BYTES, codes, run + 2L * lanes, ORDER);
            code3 = ByteVector.fromMemorySegment(BYTES, codes, run + 3L * lanes, ORDER);
            sum0 = sum0.add(code0.selectFrom(low)).add(code0.lanewise(LSHR, 4).selectFrom(high));
            sum1 = sum1.add(code1.selectFrom(low)).add(code1.lanewise(LSHR, 4).selectFrom(high));
            sum2 = sum2.add(code2.selectFrom(low)).add(code2.lanewise(LSHR, 4).selectFrom(high));
            sum3 = sum3.add(code3.selectFrom(low)).add(code3.lanewise(LSHR, 4).selectFrom(high));
            ShortVector pairs0 = sum0.reinterpretAsShorts();
            ShortVector pairs1 = sum1.reinterpretAsShorts();
            ShortVector pairs2 = sum2.reinterpretAsShorts();
            ShortVector pairs3 = sum3.reinterpretAsShorts();
            all0 = all0.add(pairs0);
            all1 = all1.add(pairs1);
            all2 = all2.add(pairs2);
            all3 = all3.add(pairs3);
            high0 = high0.add(pairs0.lanewise(LSHR, 8));
            high1 = high1.add(pairs1.lanewise(LSHR, 8));
            high2 = high2.add(pairs2.lanewise(LSHR, 8));
            high3 = high3.add(pairs3.lanewise(LSHR, 8));
          }
          all0.sub(high0.lanewise(LSHL, 8)).intoArray(sums, 0);
          all1.sub(high1.lanewise(LSHL, 8)).intoArray(sums, shorts);
          all2.sub(high2.lanewise(LSHL, 8)).intoArray(sums, 2 * shorts);
          all3.sub(high3.lanewise(LSHL, 8)).intoArray(sums, 3 * shorts);
          high0.intoArray(sums, 4 * shorts);
          high1.intoArray(sums, 5 * shorts);
          high2.intoArray(sums, 6 * shorts);
          high3.intoArray(sums, 7 * shorts);
          // The vector at position p of a block's runs holds, by the interleaving, consecutive
          // rows in its low bytes, and the rows half of INTERLEAVED_ROWS later in its high bytes.
          for (int g = 0; g < GROUP; g++) {
            int position = group + g * lanes;
            int start =
                first + position - position % INTERLEAVED_ROWS + position % INTERLEAVED_ROWS / 2;
            for (int half = 0; half < 2; half++) {
              ShortVector values = ShortVector.fromArray(SHORTS, sums, (half * GROUP + g) * shorts);
              for (int part = 0; part < 2; part++) {
                // tableBin, a lane at a time.
                FloatVector estimates =
                    (FloatVector) values.convertShape(S2I, INTS, part).convert(I2F, 0);
                IntVector bins =
                    ((IntVector) estimates.mul(binSlope).add(binBase).convert(F2I, 0))
                        .max(0)
                        .min(maxBin);
                int row = start + half * (INTERLEAVED_ROWS / 2) + part * INTS.length();
                if (row + INTS.length() <= rows) {
                  bins.intoArray(out, row);
                } else if (row < rows) {
                  bins.intoArray(out, row, INTS.indexInRange(row, rows));
                }
              }
            }
          }
        }
      }
    }

    // ---- INT8 dot products.

    @Override
    int dotProduct(byte[] query, byte[] records, int offset) {
      if (DOT_IS_256 == false) {
        byte[] code = fallbackCode(query.length);
        System.arraycopy(records, offset, code, 0, query.length);
        return VectorUtil.dotProduct(query, code);
      }
      IntVector sum = IntVector.zero(DOT_INTS);
      int end = DOT_BYTES.loopBound(query.length);
      int i = 0;
      for (; i < end; i += DOT_BYTES.length()) {
        sum =
            sum.add(
                ((IntVector)
                        ByteVector.fromArray(DOT_BYTES, query, i).convertShape(B2I, DOT_INTS, 0))
                    .mul(
                        (IntVector)
                            ByteVector.fromArray(DOT_BYTES, records, offset + i)
                                .convertShape(B2I, DOT_INTS, 0)));
      }
      int total = sum.reduceLanes(ADD);
      for (; i < query.length; i++) {
        total += query[i] * records[offset + i];
      }
      return total;
    }

    @Override
    void dotProducts(byte[] query, byte[] records, int stride, int count, int[] out) {
      if (DOT_IS_256 == false) {
        byte[] code = fallbackCode(query.length);
        for (int r = 0; r < count; r++) {
          System.arraycopy(records, r * stride, code, 0, query.length);
          out[r] = VectorUtil.dotProduct(query, code);
        }
        return;
      }
      int end = DOT_BYTES.loopBound(query.length);
      int r = 0;
      for (; r + 4 <= count; r += 4) {
        int row0 = r * stride;
        int row1 = row0 + stride;
        int row2 = row1 + stride;
        int row3 = row2 + stride;
        IntVector sum0 = IntVector.zero(DOT_INTS);
        IntVector sum1 = IntVector.zero(DOT_INTS);
        IntVector sum2 = IntVector.zero(DOT_INTS);
        IntVector sum3 = IntVector.zero(DOT_INTS);
        int i = 0;
        for (; i < end; i += DOT_BYTES.length()) {
          IntVector q =
              ((IntVector)
                  ByteVector.fromArray(DOT_BYTES, query, i).convertShape(B2I, DOT_INTS, 0));
          sum0 =
              sum0.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromArray(DOT_BYTES, records, row0 + i)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum1 =
              sum1.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromArray(DOT_BYTES, records, row1 + i)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum2 =
              sum2.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromArray(DOT_BYTES, records, row2 + i)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum3 =
              sum3.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromArray(DOT_BYTES, records, row3 + i)
                              .convertShape(B2I, DOT_INTS, 0)));
        }
        int score0 = sum0.reduceLanes(ADD);
        int score1 = sum1.reduceLanes(ADD);
        int score2 = sum2.reduceLanes(ADD);
        int score3 = sum3.reduceLanes(ADD);
        for (; i < query.length; i++) {
          int q = query[i];
          score0 += q * records[row0 + i];
          score1 += q * records[row1 + i];
          score2 += q * records[row2 + i];
          score3 += q * records[row3 + i];
        }
        out[r] = score0;
        out[r + 1] = score1;
        out[r + 2] = score2;
        out[r + 3] = score3;
      }
      for (; r < count; r++) {
        out[r] = dotProduct(query, records, r * stride);
      }
    }

    @Override
    void dotProducts(byte[] query, MemorySegment records, long[] offsets, int count, int[] out) {
      if (DOT_IS_256 == false) {
        byte[] code = fallbackCode(query.length);
        for (int r = 0; r < count; r++) {
          MemorySegment.copy(records, ValueLayout.JAVA_BYTE, offsets[r], code, 0, query.length);
          out[r] = VectorUtil.dotProduct(query, code);
        }
        return;
      }
      int end = DOT_BYTES.loopBound(query.length);
      int r = 0;
      for (; r + 4 <= count; r += 4) {
        long row0 = offsets[r];
        long row1 = offsets[r + 1];
        long row2 = offsets[r + 2];
        long row3 = offsets[r + 3];
        IntVector sum0 = IntVector.zero(DOT_INTS);
        IntVector sum1 = IntVector.zero(DOT_INTS);
        IntVector sum2 = IntVector.zero(DOT_INTS);
        IntVector sum3 = IntVector.zero(DOT_INTS);
        for (int i = 0; i < end; i += DOT_BYTES.length()) {
          IntVector q =
              ((IntVector)
                  ByteVector.fromArray(DOT_BYTES, query, i).convertShape(B2I, DOT_INTS, 0));
          sum0 =
              sum0.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromMemorySegment(
                                  DOT_BYTES, records, row0 + i, ByteOrder.LITTLE_ENDIAN)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum1 =
              sum1.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromMemorySegment(
                                  DOT_BYTES, records, row1 + i, ByteOrder.LITTLE_ENDIAN)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum2 =
              sum2.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromMemorySegment(
                                  DOT_BYTES, records, row2 + i, ByteOrder.LITTLE_ENDIAN)
                              .convertShape(B2I, DOT_INTS, 0)));
          sum3 =
              sum3.add(
                  q.mul(
                      (IntVector)
                          ByteVector.fromMemorySegment(
                                  DOT_BYTES, records, row3 + i, ByteOrder.LITTLE_ENDIAN)
                              .convertShape(B2I, DOT_INTS, 0)));
        }
        out[r] = sum0.reduceLanes(ADD) + scalarDot(query, records, row0, end);
        out[r + 1] = sum1.reduceLanes(ADD) + scalarDot(query, records, row1, end);
        out[r + 2] = sum2.reduceLanes(ADD) + scalarDot(query, records, row2, end);
        out[r + 3] = sum3.reduceLanes(ADD) + scalarDot(query, records, row3, end);
      }
      for (; r < count; r++) {
        IntVector sum = IntVector.zero(DOT_INTS);
        for (int i = 0; i < end; i += DOT_BYTES.length()) {
          sum =
              sum.add(
                  ((IntVector)
                          ByteVector.fromArray(DOT_BYTES, query, i).convertShape(B2I, DOT_INTS, 0))
                      .mul(
                          (IntVector)
                              ByteVector.fromMemorySegment(
                                      DOT_BYTES, records, offsets[r] + i, ByteOrder.LITTLE_ENDIAN)
                                  .convertShape(B2I, DOT_INTS, 0)));
        }
        out[r] = sum.reduceLanes(ADD) + scalarDot(query, records, offsets[r], end);
      }
    }
  }
}
