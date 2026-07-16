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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.ObjectCursor;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers one field's vectors during flush, de-duplicating them through a shared {@link
 * DedupGroup}.
 *
 * @lucene.experimental
 */
final class DedupFlatFieldVectorsWriter<T> extends FlatFieldVectorsWriter<T> {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatFieldVectorsWriter.class);

  private final DedupGroup<T> group;
  private final DocsWithFieldSet docsWithFieldSet;
  private final List<T> vectors;
  private final IntArrayList ordToVecOrd;
  private int lastDocID;
  private boolean finished;

  DedupFlatFieldVectorsWriter(DedupGroup<T> group) {
    this.group = group;
    this.docsWithFieldSet = new DocsWithFieldSet();
    this.vectors = new ArrayList<>();
    this.ordToVecOrd = new IntArrayList();
    this.lastDocID = -1;
    this.finished = false;
  }

  @Override
  public List<T> getVectors() {
    return vectors;
  }

  @Override
  public DocsWithFieldSet getDocsWithFieldSet() {
    return docsWithFieldSet;
  }

  IntArrayList getOrdToVecOrd() {
    return ordToVecOrd;
  }

  @Override
  public void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public T copyValue(T vectorValue) {
    throw new UnsupportedOperationException(); // handled inside group
  }

  @Override
  public void addValue(int docID, T vectorValue) throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    } else if (docID <= lastDocID) {
      throw new IllegalArgumentException(
          "docID=" + docID + " not going forwards, indexed lastDocID=" + lastDocID);
    }

    lastDocID = docID;
    docsWithFieldSet.add(docID);

    ObjectCursor<T> cursor = group.addUnique(vectorValue);
    vectors.add(cursor.value); // owned vector value
    ordToVecOrd.add(cursor.index); // index in group
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + docsWithFieldSet.ramBytesUsed()
        + (long) vectors.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + ordToVecOrd.ramBytesUsed();
  }
}
