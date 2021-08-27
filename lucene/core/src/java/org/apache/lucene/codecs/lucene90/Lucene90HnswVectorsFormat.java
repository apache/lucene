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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Lucene 9.0 vector format, which encodes numeric vector values and an optional associated graph
 * connecting the documents having values. The graph is used to power HNSW search. The format
 * consists of three files:
 *
 * <h2>.vec (vector data) file</h2>
 *
 * <p>This file stores all the floating-point vector data ordered by field, document ordinal, and
 * vector dimension. The floats are stored in little-endian byte order.
 *
 * <h2>.vex (vector index) file</h2>
 *
 * <p>Stores graphs connecting the documents for each field. For each document having a vector for a
 * given field, this is stored as:
 *
 * <ul>
 *   <li><b>[int32]</b> the number of neighbor nodes
 *   <li><b>array[vint]</b> the neighbor ordinals, delta-encoded (initially subtracting -1)
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
 *   <li><b>array[vint]</b> the docids of documents having vectors, in order
 *   <li><b>array[vlong]</b> for each document having a vector, the offset (delta-encoded relative
 *       to the previous document) of its entry in the .vex file
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene90HnswVectorsFormat extends KnnVectorsFormat {

  static final String META_CODEC_NAME = "Lucene90HnswVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene90HnswVectorsFormatData";
  static final String VECTOR_INDEX_CODEC_NAME = "Lucene90HnswVectorsFormatIndex";
  static final String META_EXTENSION = "vem";
  static final String VECTOR_DATA_EXTENSION = "vec";
  static final String VECTOR_INDEX_EXTENSION = "vex";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  public static final int DEFAULT_MAX_CONN = 16;
  public static final int DEFAULT_BEAM_WIDTH = 16;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link Lucene90HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to to {@link Lucene90HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link
   * HnswGraph} for details.
   */
  private final int beamWidth;

  public Lucene90HnswVectorsFormat() {
    this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
  }

  public Lucene90HnswVectorsFormat(int maxConn, int beamWidth) {
    super("Lucene90HnswVectorsFormat");
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene90HnswVectorsWriter(state, maxConn, beamWidth);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene90HnswVectorsReader(state);
  }
}
