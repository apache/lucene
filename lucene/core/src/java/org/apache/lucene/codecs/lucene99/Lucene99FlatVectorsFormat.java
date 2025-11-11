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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
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
 * Lucene 9.9 flat vector format, which encodes numeric vector values
 *
 * <h2>.vec (vector data) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li>Vector data ordered by field, document ordinal, and vector dimension. When the
 *       vectorEncoding is BYTE, each sample is stored as a single byte. When it is FLOAT32, each
 *       sample is stored as an IEEE float in little-endian byte order.
 *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
 *       note that only in sparse case
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}Lucene,
 *       note that only in sparse case
 * </ul>
 *
 * <h2>.vemf (vector metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[vlong]</b> offset to this field's vectors in the .vec file
 *   <li><b>[vlong]</b> length of this field's vectors, in bytes
 *   <li><b>[vint]</b> dimension of this field's vectors
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[int64]</b> docsWithFieldOffset: if equals to -2, empty - no vector values. If equals to
 *       -1, dense – all documents have values for a field. If &gt;= 0, sparse – some documents
 *       missing values, and this value is the offset to the docsWithField bitset in the main data
 *       (.vec) file. If equals to -3, dense *and* vectors have been reordered.
 *   <li><b>[int64]</b> docsWithFieldLength: 0, or the length of the docsWithField bitset when
 *       sparse.
 *   <li><b>[int16]</b> jumpTableEntryCount: used when sparse; otherwise -1.
 *   <li><b>[int8]</b> denseRankPower: used when sparse; otherwise -1.
 *   <li>DocIds were encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}
 *   <li>When Sparse and monotonically ordered:
 *   <li><b>[int64]</b> addressesOffset: pointer to OrdToDoc in vector data.
 *   <li><b>[int64]</b> addressesLength: length of OrdToDoc in vector data.
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}.
 *   <li>When re-ordered:
 *   <li><b>[int64]</b> addressesOffset: pointer to OrdToDoc in vector data.
 *   <li><b>[int64]</b> addressesLength: length of OrdToDoc in vector data.
 *   <li><b>[int64]</b> docToOrdLength: length of DocToOrd in vector data.
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectWriter}.
 *   <li>DocToOrd was encoded by {@link org.apache.lucene.util.GroupVIntUtil}.
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene99FlatVectorsFormat extends FlatVectorsFormat {

  static final String NAME = "Lucene99FlatVectorsFormat";
  static final String META_CODEC_NAME = "Lucene99FlatVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene99FlatVectorsFormatData";
  static final String META_EXTENSION = "vemf";
  static final String VECTOR_DATA_EXTENSION = "vec";

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
  private final FlatVectorsScorer vectorsScorer;
  private final boolean enableReorder;

  /** Constructs a format */
  public Lucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
    super(NAME);
    this.vectorsScorer = vectorsScorer;
    this.enableReorder = false;
  }

  Lucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer, boolean enableReorder) {
    super(NAME);
    this.vectorsScorer = vectorsScorer;
    this.enableReorder = enableReorder;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99FlatVectorsWriter(state, vectorsScorer, enableReorder);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99FlatVectorsReader(state, vectorsScorer);
  }

  @Override
  public String toString() {
    return "Lucene99FlatVectorsFormat(" + "vectorsScorer=" + vectorsScorer + ')';
  }
}
