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
package org.apache.lucene.sandbox.codecs.ivfaster;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * The fine tier: the per-dimension int8 code that ranks whatever the coarse tier admitted.
 *
 * <p>The shipped quantized tier is {@code Int8Quantizer}: full 8-bit doc levels, one byte per
 * dimension, scored by the unsigned int8 dot (the codec's own {@code BulkDotKernel}) with an exact
 * offset correction. {@code Fp32Quantizer} implements the same interface for the exact-FP32-rerank
 * path. The interface exists so the tier stays a selectable, self-describing property of the index
 * rather than a compiled-in constant.
 *
 * <h2>Contract</h2>
 *
 * <p>An implementation owns its own code layout entirely: the meaning of the {@code code} bytes and
 * of the four trailing per-record floats is private to it. The writer stores whatever {@link
 * #encode} produces and the reader hands it back unchanged, so on-disk records are opaque to
 * everything except the quantizer that wrote them.
 *
 * <p>Dispatch is on the PERSISTED {@link #encodingId()}, never on an ambient system property, so a
 * segment is self-describing: an index built by one configuration reads correctly under another.
 *
 * <p>Implementations must be immutable and safe for concurrent use by multiple search threads. The
 * per-query state produced by {@link #prepareQuery} is NOT shared: it belongs to the caller, one
 * per query.
 *
 * @lucene.experimental
 */
interface FineQuantizer {

  /** Identifies the encoding on disk. Persisted per field and used to dispatch reranking. */
  byte encodingId();

  /** Human-readable name, for {@code toString} and for the benchmark harness's index key. */
  String name();

  /** Bytes of code per document at this dimension, excluding the trailing per-record floats. */
  int codeBytes(int dim);

  /**
   * Whether this tier wants its rerank batch staged as one FLAT buffer at a constant stride, rather
   * than as {@code byte[][]} rows.
   *
   * <p>True only for a tier that overrides {@link QueryState#scoreBulkStrided}. The two layouts
   * read the SAME bytes, since the reader copies one record per candidate either way, so this
   * decides only the destination. Getting it wrong costs a copy rather than correctness: a tier
   * that inherits the default and is handed a flat buffer has the default copy it back out into
   * rows, adding a second copy of the whole shortlist.
   *
   * <p>Declared here rather than inferred reflectively from the state class so that the answer
   * lives next to the override it describes.
   *
   * <p>Default false: the rows layout is correct for every tier, just without the native batch.
   */
  default boolean wantsStridedStaging() {
    return false;
  }

  /**
   * Whether this encoding can represent the given dimension. Both shipped tiers store one byte per
   * dimension and support every dimension, so the default is {@code true}; a tier with a structural
   * width constraint (e.g. a bit-sliced code needing whole 64-bit words) overrides this, and
   * callers fall back to an exact float comparison where it returns {@code false}.
   */
  default boolean supports(int dim) {
    return true;
  }

  /**
   * Whether this encoding scores against mean-centred document vectors.
   *
   * <p>When true the writer computes the corpus mean, subtracts it before {@link #encode}, and
   * persists it; the reader passes it to {@link #prepareQuery}. The QUERY is never centred; see
   * {@link #prepareQuery}.
   */
  boolean needsMean();

  /**
   * Encodes one rotated document vector.
   *
   * @param vector the rotated vector, already mean-centred when {@link #needsMean()}
   * @param dim dimension
   * @param mean the corpus mean, or {@code null} when {@link #needsMean()} is false. Passed
   *     EXPLICITLY rather than left implicit in {@code vector}: an encoding whose grid depends on
   *     the mean must derive that grid exactly as the query side does, and a centred vector alone
   *     does not carry enough information to do so.
   * @param code receives {@code codeBytes(dim)} bytes
   * @param corrections receives four per-record floats, whose meaning is private to the
   *     implementation; they are stored verbatim after the code and handed back to {@link
   *     QueryState#scoreBulk} unchanged
   */
  void encode(float[] vector, int dim, float[] mean, byte[] code, float[] corrections);

  /**
   * Reconstructs one document's ROTATED vector from its code, approximately.
   *
   * <p>THE INVERSE OF {@link #encode}, and it must be implemented by every tier, because the caller
   * cannot do it: the code bytes and the four correction floats are private to the implementation,
   * so only the implementation knows whether a byte is an int8 level or one bit of eight bit
   * planes.
   *
   * <p>Two callers, both of which get silently wrong answers if this is approximated by assuming a
   * layout: the merge path, which needs floats to recompute the Lloyd mean, and {@code
   * getFloatVectorValues}, which is what an outside reader, including a benchmark computing ground
   * truth, sees when it asks this codec for a vector. Decoding a bit-sliced code as int8 bytes
   * produces finite plausible floats and throws nothing, so the failure appears only as a collapsed
   * recall.
   *
   * <p>The result is LOSSY by exactly the tier's quantization and is not normalized; the caller
   * normalizes, since both consumers want unit vectors and the norm is not recoverable from the
   * code.
   *
   * @param code the code bytes for one document
   * @param codeOffset offset of the code run within {@code code}
   * @param dim dimension
   * @param mean the persisted corpus mean, or {@code null} when {@link #needsMean()} is false.
   *     Added back here: {@link #encode} receives an already-centred vector, so a faithful inverse
   *     must undo that centring rather than leave the caller to remember it.
   * @param corrections the four floats {@link #encode} produced for this document
   * @param dest receives {@code dim} floats in ROTATED space
   */
  void decode(
      byte[] code, int codeOffset, int dim, float[] mean, float[] corrections, float[] dest);

  /**
   * Prepares per-query state.
   *
   * <p>THE QUERY IS NOT CENTRED, even when {@link #needsMean()}. The decomposition {@code dot(q, v)
   * = dot(q, mean) + dot(q, v - mean)} removes the mean from the DOCUMENT only; the {@code dot(q,
   * mean)} term is a per-query constant the implementation folds in. Centring both sides instead is
   * not a harmless symmetry: it subtracts the mean's contribution twice and the estimator's
   * correlation with the true dot collapses.
   *
   * @param rotated the rotated query, NOT centred
   * @param mean the persisted corpus mean, or {@code null} when {@link #needsMean()} is false
   */
  QueryState prepareQuery(float[] rotated, int dim, float[] mean, VectorSimilarityFunction sim);

  /**
   * Per-query scoring state. Not thread-safe: one instance per query, owned by its caller.
   *
   * <p>Scoring is split from the quantizer so that per-query setup (quantizing the query, computing
   * its scale, folding in the mean term) is paid once and amortized across the whole shortlist
   * rather than once per candidate.
   */
  interface QueryState {

    /**
     * Scores a batch of candidates, each held as a whole record with its code at a fixed offset.
     *
     * <p>BULK BY DESIGN. The per-candidate cost is dominated by preparing the QUERY side of the
     * inner loop, such as widening its int8 lanes, and that work is shared across a batch. A
     * one-candidate-at-a-time interface would repay it for every candidate in the shortlist.
     *
     * <p>Records are passed as whole rows so the reader can score straight out of what it read,
     * without copying each candidate's code into its own array.
     *
     * @param records the first {@code count} entries are candidate records
     * @param count number of candidates to score
     * @param codeOffset offset of the code run within each record
     * @param corrections {@code [count][4]} per-record floats, as written by {@link #encode}
     * @param scores receives {@code count} similarities, already mapped into the collector's scale
     *     for the active {@link VectorSimilarityFunction}
     */
    void scoreBulk(
        byte[][] records, int count, int codeOffset, float[][] corrections, float[] scores);

    /**
     * Scores a batch whose codes all live in ONE flat table, the i-th starting at {@code
     * offsets[i]}.
     *
     * <p>WHY THIS EXISTS ALONGSIDE {@link #scoreBulk}. That one takes {@code byte[][]}, which
     * forces any caller holding a contiguous table to copy each candidate's code into its own row
     * first. The centroid tier is exactly that shape, {@code nlist * fineBytes} in one array, so
     * satisfying the signature would cost a full-code copy per candidate for a score that costs
     * less than the copy.
     *
     * <p>Default delegates by gathering into rows, so an implementation with nothing to gain stays
     * correct; overriding it is what avoids the copy.
     *
     * @param table the flat code table
     * @param offsets byte offset of each candidate's code within {@code table}; length >= {@code
     *     count}
     * @param scratch {@code [>=count][>=codeBytes]} rows for the default gathering path; may be
     *     null when the implementation overrides this and needs no gather
     */
    default void scoreBulkAt(
        byte[] table,
        int[] offsets,
        int count,
        int codeBytes,
        float[][] corrections,
        float[] scores,
        byte[][] scratch) {
      for (int i = 0; i < count; i++) {
        System.arraycopy(table, offsets[i], scratch[i], 0, codeBytes);
      }
      scoreBulk(scratch, count, 0, corrections, scores);
    }

    /**
     * Scores a batch whose records are laid out at a CONSTANT STRIDE in one flat buffer: candidate
     * {@code i}'s record begins at {@code recs[i * stride]} and its code at {@code + codeOffset}.
     *
     * <p>WHY THIS EXISTS ALONGSIDE {@link #scoreBulk}. It is the shape a NATIVE kernel needs: an
     * FFM downcall per candidate costs about as much as the arithmetic it performs, so the win is
     * in amortizing the call over the whole shortlist, and a {@code byte[][]} cannot express one
     * call over n rows to native code, since the rows are separate objects at unrelated addresses.
     * A flat buffer with a stride can.
     *
     * <p>This costs the reader nothing. It already copies each candidate's record out of the mapped
     * code table, and that copy is load-bearing, since it turns a scattered read into a sequential
     * one. Copying those bytes into one flat buffer rather than into {@code n} rows is the same
     * bytes to a different destination.
     *
     * <p>The default gathers into rows and delegates, so an implementation with nothing to gain
     * stays correct.
     *
     * @param recs flat buffer holding {@code count} records of {@code stride} bytes
     * @param scratch {@code [>=count][>=stride]} rows for the default gathering path; may be null
     *     when the implementation overrides this
     */
    default void scoreBulkStrided(
        byte[] recs,
        int count,
        int stride,
        int codeOffset,
        float[][] corrections,
        float[] scores,
        byte[][] scratch) {
      for (int i = 0; i < count; i++) {
        System.arraycopy(recs, i * stride, scratch[i], 0, stride);
      }
      scoreBulk(scratch, count, codeOffset, corrections, scores);
    }

    /**
     * Switches this state to writing the quantized DOT PRODUCT rather than a collector-scaled
     * score.
     *
     * <p>For callers doing geometry rather than collecting: a distance derived from a similarity is
     * affine in the dot, so it ranks identically while carrying a different scale, and mixing the
     * two produces magnitudes that are wrong by a constant factor without ever being wrong in
     * order.
     *
     * <p>Default is a no-op: an implementation whose score already IS the dot has nothing to
     * switch.
     */
    default void reportRawDots() {}

    /**
     * Re-targets this state at a new query vector, reusing its buffers, and returns whether it
     * could.
     *
     * <p>For loops that prepare state per vector, as routing does once per document per pass, where
     * the allocation and zero-fill dominate the preparation. Returning {@code false} means the
     * caller must build fresh state, so an implementation with nothing reusable need not implement
     * this.
     */
    default boolean reset(float[] rotated, float[] mean) {
      return false;
    }
  }
}
