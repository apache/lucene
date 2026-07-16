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
package org.apache.lucene.codecs.lucene106.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Flat vector format that stores each distinct vector once.
 *
 * <p>Vectors that share the same dimension and encoding form a <i>group</i>. Within a group, an
 * identical vector is stored a single time regardless of how many documents (across all fields that
 * map to that group) reference it; each field then keeps a per-document {@code ordToVecOrd} map
 * from its document ordinal to the group ordinal of the shared vector. This is well suited to
 * indexes with repeated vectors, e.g. several fields derived from the same embedding, or heavily
 * duplicated content.
 *
 * <h2>.vdd (vector de-dup data) file</h2>
 *
 * <ul>
 *   <li>For each group, its distinct vectors, aligned to 4 bytes (BYTE) or 64 bytes (FLOAT32).
 *   <li>For each field:
 *       <ul>
 *         <li>The sparse-encoding data (only when some documents lack the field): DocIds encoded by
 *             {@link
 *             org.apache.lucene.codecs.lucene90.IndexedDISI#writeBitSet(org.apache.lucene.search.DocIdSetIterator,
 *             org.apache.lucene.store.IndexOutput, byte)}, followed by the ordinal-to-doc mapping
 *             encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}.
 *         <li>The {@code ordToVecOrd} map (aligned to 4 bytes): one entry per document ordinal
 *             giving the group ordinal of the shared vector, packed by {@link
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
 *   <li><b>[int64]</b> offset to this field's {@code ordToVecOrd} map in the .vdd file
 *   <li><b>[int64]</b> length of this field's {@code ordToVecOrd} map, in bytes
 * </ul>
 *
 * <p>also terminated by <b>[int32]</b> {@code -1}.
 *
 * <h2>Complexity</h2>
 *
 * <p>Let {@code N} be the number of indexed vectors (one per document per field), {@code U} the
 * number of distinct vectors, and {@code d} the dimension. De-duplication interns each vector via a
 * hash lookup with linear probing, resolving hash collisions with a full equality check.
 *
 * <ul>
 *   <li><b>Indexing (flush):</b> expected {@code O(N * d)} time (a hash plus occasional equality
 *       check per vector). Heap is {@code O(U * d)} for the distinct vectors held in the group,
 *       plus {@code O(N)} for the per-document references and {@code ordToVecOrd} entries.
 *   <li><b>Merge:</b> expected {@code O(N * d)} time; distinct vectors are written to disk as soon
 *       as they are first seen rather than buffered, so heap stays {@code O(N)} (the per-field
 *       {@code ordToVecOrd} maps and light per-vector handles) with no {@code O(U * d)} term. When
 *       a source segment is itself in this format, equality is decided by comparing group ordinals
 *       in {@code O(1)} without reading the vectors back.
 *   <li><b>Reading:</b> both the vectors and the {@code ordToVecOrd} map stay off-heap; a read
 *       resolves a document ordinal to its vector via one extra {@code ordToVecOrd} lookup.
 * </ul>
 *
 * @lucene.experimental
 */
final class DedupFlatVectorsFormat extends FlatVectorsFormat {
  static final String NAME = "Lucene106DedupFlatVectorsFormat";

  static final String META_CODEC_NAME = "Lucene106DedupFlatVectorsFormatMeta";
  static final String META_EXTENSION = "vdm";

  static final String VECTOR_DATA_CODEC_NAME = "Lucene106DedupFlatVectorsFormatVectorData";
  static final String VECTOR_DATA_EXTENSION = "vdd";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  private static final DedupFlatVectorsScorer FLAT_VECTORS_SCORER = new DedupFlatVectorsScorer();

  DedupFlatVectorsFormat() {
    super(NAME);
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupFlatVectorsWriter(state, FLAT_VECTORS_SCORER);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupFlatVectorsReader(state, FLAT_VECTORS_SCORER);
  }
}
