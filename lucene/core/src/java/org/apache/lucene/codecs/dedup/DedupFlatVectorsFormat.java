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

package org.apache.lucene.codecs.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;

/**
 * Flat vector format that stores each unique vector exactly once per segment. Multiple documents
 * (within the same field, and across fields with matching {@code (dimension, encoding)}) may share
 * the same physical vector, dramatically reducing on-disk size when many docs/fields point at the
 * same vector data.
 *
 * <p>The format groups fields into "pools" keyed by {@code (dimension, encoding)}. All fields in
 * the same pool share unique-vector storage; per-field metadata records how each doc-ord maps to a
 * vector-ord within the pool.
 *
 * <h2>.dvc (deduped vector data) file</h2>
 *
 * <ul>
 *   <li>For each pool, in pool-id order:
 *       <ul>
 *         <li>64-byte aligned for FLOAT32 (4-byte aligned for BYTE)
 *         <li>{@code uniqueCount * dim * byteSize} raw vector bytes, addressed by vec-ord
 *       </ul>
 *   <li>For each field, in declaration order:
 *       <ul>
 *         <li>If sparse: DISI bitset for docsWithField, written by {@link
 *             IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}, plus a {@link
 *             org.apache.lucene.util.packed.DirectMonotonicWriter} ord-to-doc table.
 *         <li>Packed {@code docOrd -> vecOrd} map, written by {@link
 *             org.apache.lucene.util.packed.DirectWriter} with {@code ceil(log2(poolUniqueCount))}
 *             bits per value.
 *       </ul>
 * </ul>
 *
 * <h2>.dvm (deduped vector metadata) file</h2>
 *
 * <ul>
 *   <li>{@code [vint] numPools}
 *   <li>For each pool:
 *       <ul>
 *         <li>{@code [vint]} dim
 *         <li>{@code [byte]} encoding ordinal
 *         <li>{@code [vlong]} vectorDataOffset
 *         <li>{@code [vlong]} vectorDataLength
 *         <li>{@code [vint]} uniqueCount
 *       </ul>
 *   <li>For each field, terminated by sentinel field number {@code -1}:
 *       <ul>
 *         <li>{@code [int]} field number
 *         <li>{@code [byte]} similarity ordinal
 *         <li>{@code [vint]} pool id
 *         <li>{@code [int]} doc cardinality
 *         <li>DISI mode + sparse blocks (see {@link
 *             org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration})
 *         <li>{@code [vlong]} mapOffset
 *         <li>{@code [vlong]} mapLength
 *         <li>{@code [byte]} bitsPerVecOrd
 *       </ul>
 * </ul>
 *
 * @lucene.experimental
 */
public final class DedupFlatVectorsFormat extends FlatVectorsFormat {

  static final String NAME = "DedupFlatVectorsFormat";
  static final String META_CODEC_NAME = "DedupFlatVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "DedupFlatVectorsFormatData";
  static final String META_EXTENSION = "dvm";
  static final String VECTOR_DATA_EXTENSION = "dvc";

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  /**
   * {@link org.apache.lucene.util.packed.DirectMonotonicWriter} block shift used for the ord-to-doc
   * table on sparse fields.
   */
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private final FlatVectorsScorer vectorsScorer;

  /** Constructs a format using the platform-optimal Lucene99 scorer. */
  public DedupFlatVectorsFormat() {
    this(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
  }

  /** Constructs a format with an explicit scorer. */
  public DedupFlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
    super(NAME);
    this.vectorsScorer = vectorsScorer;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupFlatVectorsWriter(state, vectorsScorer);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupFlatVectorsReader(state, vectorsScorer);
  }

  @Override
  public String toString() {
    return "DedupFlatVectorsFormat(vectorsScorer=" + vectorsScorer + ")";
  }
}
