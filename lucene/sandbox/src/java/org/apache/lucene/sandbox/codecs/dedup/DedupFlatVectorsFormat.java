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
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Flat vector format that stores each distinct vector of a document once.
 *
 * <p>Vectors that share the same dimension and encoding form a <i>group</i>. Within a group, an
 * identical vector is stored a single time per document regardless of how many of its fields (among
 * all fields that map to that group) reference it; each field then keeps a {@code
 * fieldOrdToGroupOrd} map from its document ordinal to the group ordinal of the shared vector. This
 * is well suited to indexes with several fields derived from the same embedding. Identical vectors
 * of different documents are stored once each, which keeps the group's vectors in doc order.
 *
 * <h2>.vdd (vector de-dup data) file</h2>
 *
 * <ul>
 *   <li>For each group, its distinct vectors in doc order, aligned to 4 bytes (BYTE) or 64 bytes
 *       FLOAT16). This a best-effort alignment, because Arm Neoverse machines incur a performance
 *       penalty in reading data not aligned to 64 bytes. This penalty may be incurred for float
 *       vectors that do not have a dimension of a multiple of 16 (equivalent to 64 bytes), because
 *       the alignment will not hold for all vectors in the file.
 *   <li>For each field:
 *       <ul>
 *         <li>The sparse-encoding data (only when some documents lack the field): DocIds encoded by
 *             {@link
 *             org.apache.lucene.codecs.lucene90.IndexedDISI#writeBitSet(org.apache.lucene.search.DocIdSetIterator,
 *             org.apache.lucene.store.IndexOutput, byte)}, followed by the ordinal-to-doc mapping
 *             encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}.
 *         <li>The {@code fieldOrdToGroupOrd} map (aligned to 4 bytes): one entry per document
 *             ordinal giving the group ordinal of the shared vector, packed by {@link
 *             org.apache.lucene.util.packed.DirectWriter}.
 *       </ul>
 * </ul>
 *
 * <h2>.vdm (vector de-dup metadata) file</h2>
 *
 * <p>A list of groups, each:
 *
 * <ul>
 *   <li><b>[int32]</b> group ordinal
 *   <li><b>[int32]</b> vector dimension
 *   <li><b>[int32]</b> vector encoding ordinal
 *   <li><b>[int32]</b> group size (number of distinct vectors)
 *   <li><b>[int64]</b> offset to this group's vectors in the .vdd file
 *   <li><b>[int64]</b> length of this group's vectors, in bytes
 * </ul>
 *
 * <p>terminated by <b>[int32]</b> {@code -1}, then a list of fields, each:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[int32]</b> vector dimension
 *   <li><b>[int32]</b> vector encoding ordinal
 *   <li><b>[int32]</b> ordinal of the group holding this field's vectors
 *   <li><b>[int32]</b> the number of documents having values for this field
 *   <li>the sparse-encoding metadata (docs-with-field offset/length and ordToDoc configuration), as
 *       written by {@link
 *       org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration#writeStoredMeta}
 *   <li><b>[int64]</b> offset to this field's {@code fieldOrdToGroupOrd} map in the .vdd file
 *   <li><b>[int64]</b> length of this field's {@code fieldOrdToGroupOrd} map, in bytes
 *   <li><b>[int32]</b> bits per value used to pack this field's {@code fieldOrdToGroupOrd} map
 * </ul>
 *
 * <p>also terminated by <b>[int32]</b> {@code -1}.
 *
 * <h2>Complexity</h2>
 *
 * <p>Let {@code N} be the number of indexed vectors (one per document per field), {@code U} the
 * number of distinct vectors per document summed over all documents, {@code F} the number of fields
 * in a group, and {@code d} the dimension. A vector is only compared with the other vectors of its
 * document, so there is no hash table; de-duplication relies on the vectors of a document being
 * added together, as {@link org.apache.lucene.index.IndexWriter#addDocument} does.
 *
 * <ul>
 *   <li><b>Indexing (flush):</b> {@code O(N * F * d)} time at worst (up to {@code F - 1} equality
 *       checks per vector). Heap is {@code O(U * d)} for the vectors held in the group, plus {@code
 *       O(N)} for the per-document references and {@code fieldOrdToGroupOrd} entries. The group's
 *       vectors are written in (index-sorted) doc order.
 *   <li><b>Merge:</b> {@code O(N * F * d)} time at worst; the fields of a group are merged together
 *       in doc order, and a vector is written to disk as soon as it is first seen in its document
 *       rather than buffered, so heap stays {@code O(N)} (the per-field {@code fieldOrdToGroupOrd}
 *       maps and light per-vector handles) with no {@code O(U * d)} term. Since the vectors of a
 *       source segment are in doc order, they are read sequentially, through an input opened for
 *       merges if the reader's own input reads at random, and a vector is only compared with the
 *       vectors of its document that were just read.
 *   <li><b>Reading:</b> both the vectors and the {@code fieldOrdToGroupOrd} map stay off-heap; a
 *       read resolves a document ordinal to its vector via one extra {@code fieldOrdToGroupOrd}
 *       lookup.
 * </ul>
 *
 * @lucene.experimental
 */
final class DedupFlatVectorsFormat extends FlatVectorsFormat {
  static final String NAME = "DedupFlatVectorsFormat";

  private static final String META_CODEC_NAME = "DedupFlatVectorsFormatMeta";
  private static final String META_EXTENSION = "vdm";

  private static final String VECTOR_DATA_CODEC_NAME = "DedupFlatVectorsFormatVectorData";
  private static final String VECTOR_DATA_EXTENSION = "vdd";

  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;

  private static final DedupFlatVectorsScorer FLAT_VECTORS_SCORER = new DedupFlatVectorsScorer();

  DedupFlatVectorsFormat() {
    super(NAME);
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupFlatVectorsWriter(
        state,
        FLAT_VECTORS_SCORER,
        null,
        META_CODEC_NAME,
        META_EXTENSION,
        VECTOR_DATA_CODEC_NAME,
        VECTOR_DATA_EXTENSION,
        null,
        null,
        VERSION_CURRENT);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupFlatVectorsReader(
        state,
        FLAT_VECTORS_SCORER,
        META_CODEC_NAME,
        META_EXTENSION,
        VECTOR_DATA_CODEC_NAME,
        VECTOR_DATA_EXTENSION,
        VERSION_START,
        VERSION_CURRENT);
  }
}
