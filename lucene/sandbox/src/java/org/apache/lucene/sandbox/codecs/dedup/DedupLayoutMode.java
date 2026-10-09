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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Per-field layout mode chosen at flush time by {@link DedupHnswVectorsWriter} and read back by
 * {@link DedupHnswVectorsReader}.
 *
 * <p>The {@link #id} is the stable on-disk encoding; do not rely on {@link #ordinal()} for
 * persistence.
 *
 * @lucene.experimental
 */
enum DedupLayoutMode {
  /**
   * Builds the HNSW graph over distinct vectors (group ordinals) and stores a {@code
   * DistinctVectorPostings} to expand matched groups back to documents. Used when the field has
   * effective de-duplication.
   */
  DEDUP((byte) 0),

  /**
   * Builds a document-space graph (one node per document) with no postings, so search behaves like
   * a vanilla HNSW search. Used when the field has no effective de-duplication.
   */
  PLAIN((byte) 1),

  /**
   * Mixed layout. The graph contains one node per <b>large</b> group (a distinct vector referenced
   * by more than the configured threshold of documents) plus one node per <b>document</b> that
   * belongs to a small group. Large-group nodes carry a {@code DistinctVectorPostings} slice to
   * expand back to their documents; small-group nodes are documents themselves and carry only their
   * single field ordinal. A per-node {@code nodeToGroupOrd} map lets the scorer resolve every node
   * (large or small) to a vector in the group view. Used when a field has a few very large groups
   * but most documents are (near-)unique.
   */
  HYBRID((byte) 2);

  private final byte id;

  DedupLayoutMode(byte id) {
    this.id = id;
  }

  /** Writes the stable on-disk id. */
  void write(DataOutput out) throws IOException {
    out.writeByte(id);
  }

  /** Reads a mode from its on-disk id, throwing on an unknown value. */
  static DedupLayoutMode read(DataInput in) throws IOException {
    byte id = in.readByte();
    for (DedupLayoutMode mode : values()) {
      if (mode.id == id) {
        return mode;
      }
    }
    throw new CorruptIndexException("Unknown dedup layout mode id: " + id, in);
  }
}
