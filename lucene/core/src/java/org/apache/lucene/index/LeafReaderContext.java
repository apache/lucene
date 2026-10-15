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
package org.apache.lucene.index;

import java.util.Collections;
import java.util.List;

/** {@link IndexReaderContext} for {@link LeafReader} instances. */
public final class LeafReaderContext extends IndexReaderContext {
  /** The reader's ord in the top-level's leaves array */
  public final int ord;

  /**
   * The reader's absolute doc base: the global doc id of this leaf's first document within the
   * top-level composite reader. This is a {@code long} because a composite/directory reader may
   * span more than {@link Integer#MAX_VALUE} documents across its segments, even though each
   * segment (leaf) is itself limited to {@link Integer#MAX_VALUE} documents.
   */
  public final long docBase;

  private final LeafReader reader;
  private final List<LeafReaderContext> leaves;

  /** Creates a new {@link LeafReaderContext} */
  LeafReaderContext(
      CompositeReaderContext parent,
      LeafReader reader,
      int ord,
      long docBase,
      int leafOrd,
      long leafDocBase) {
    super(parent, ord, docBase);
    this.ord = leafOrd;
    this.docBase = leafDocBase;
    this.reader = reader;
    this.leaves = isTopLevel ? Collections.singletonList(this) : null;
  }

  LeafReaderContext(LeafReader leafReader) {
    this(null, leafReader, 0, 0, 0, 0);
  }

  @Override
  public List<LeafReaderContext> leaves() {
    if (!isTopLevel) {
      throw new UnsupportedOperationException("This is not a top-level context.");
    }
    assert leaves != null;
    return leaves;
  }

  @Override
  public List<IndexReaderContext> children() {
    return null;
  }

  @Override
  public LeafReader reader() {
    return reader;
  }

  /**
   * Converts a global doc id — from the top-level composite reader, whose space is {@code long} —
   * to a leaf-local doc id in this segment, which always fits an {@code int}. Prefer this to a raw
   * {@code (int) (globalDocId - docBase)} cast: the narrowing happens in one place, and the doc is
   * asserted to actually belong to this leaf. The doc must be in {@code [docBase, docBase +
   * maxDoc)}.
   */
  public int toLeafDocId(long globalDocId) {
    assert globalDocId >= docBase && globalDocId < docBase + reader.maxDoc()
        : "global doc "
            + globalDocId
            + " outside leaf ["
            + docBase
            + ", "
            + (docBase + reader.maxDoc())
            + ")";
    return (int) (globalDocId - docBase);
  }

  @Override
  public String toString() {
    return "LeafReaderContext(" + reader + " docBase=" + docBase + " ord=" + ord + ")";
  }
}
