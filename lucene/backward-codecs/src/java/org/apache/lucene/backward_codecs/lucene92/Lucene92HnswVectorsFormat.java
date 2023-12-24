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

package org.apache.lucene.backward_codecs.lucene92;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Lucene 9.2 vector format, which encodes numeric vector values and an optional associated graph
 * connecting the documents having values. The graph is used to power HNSW search. The format
 * consists of three files:
 *
 * <h2>.vec (vector data) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li>Floating-point vector data ordered by field, document ordinal, and vector dimension. The
 *       floats are stored in little-endian byte order
 *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
 *       note that only in sparse case
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 * </ul>
 *
 * <h2>.vex (vector index)</h2>
 *
 * <p>Stores graphs connecting the documents for each field organized as a list of nodes' neighbours
 * as following:
 *
 * <ul>
 *   <li>For each level:
 *       <ul>
 *         <li>For each node:
 *             <ul>
 *               <li><b>[int32]</b> the number of neighbor nodes
 *               <li><b>array[int32]</b> the neighbor ordinals
 *               <li><b>array[int32]</b> padding if the number of the node's neighbors is less than
 *                   the maximum number of connections allowed on this level. Padding is equal to
 *                   ((maxConnOnLevel – the number of neighbours) * 4) bytes.
 *             </ul>
 *       </ul>
 * </ul>
 *
 * <h2>.vem (vector metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[vlong]</b> offset to this field's vectors in the .vec file
 *   <li><b>[vlong]</b> length of this field's vectors, in bytes
 *   <li><b>[vlong]</b> offset to this field's index in the .vex file
 *   <li><b>[vlong]</b> length of this field's index data, in bytes
 *   <li><b>[int]</b> dimension of this field's vectors
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[int8]</b> if equals to -1, dense – all documents have values for a field. If equals to
 *       0, sparse – some documents missing values.
 *   <li>DocIds were encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 *   <li><b>[int]</b> the maximum number of connections (neighbours) that each node can have
 *   <li><b>[int]</b> number of levels in the graph
 *   <li>Graph nodes by level. For each level
 *       <ul>
 *         <li><b>[int]</b> the number of nodes on this level
 *         <li><b>array[int]</b> for levels greater than 0 list of nodes on this level, stored as
 *             the the level 0th nodes ordinals.
 *       </ul>
 * </ul>
 *
 * @lucene.experimental
 */
public class Lucene92HnswVectorsFormat extends KnnVectorsFormat {

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  static final String META_CODEC_NAME = "lucene92HnswVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "lucene92HnswVectorsFormatData";
  static final String VECTOR_INDEX_CODEC_NAME = "lucene92HnswVectorsFormatIndex";
  static final String META_EXTENSION = "vem";
  static final String VECTOR_DATA_EXTENSION = "vec";
  static final String VECTOR_INDEX_EXTENSION = "vex";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link #maxConn}. See {@link HnswGraph} for more details.
   */
  final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to {@link #DEFAULT_BEAM_WIDTH}. See {@link HnswGraph} for details.
   */
  final int beamWidth;

  /** A constructor for vectors format with default parameters */
  public Lucene92HnswVectorsFormat() {
    this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
  }

  /** Constructs a format for reading old indexes */
  public Lucene92HnswVectorsFormat(int maxConn, int beamWidth) {
    super("lucene92HnswVectorsFormat");
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("Old codecs may only be used for reading");
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene92HnswVectorsReader(state);
  }

  @Override
  public String toString() {
    return "Lucene92HnswVectorsFormat(name = Lucene92HnswVectorsFormat, maxConn = "
        + maxConn
        + ", beamWidth="
        + beamWidth
        + ")";
  }
}
