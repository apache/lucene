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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.ObjectCursor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * Stores the vectors of a group once per document: {@link #addUnique} returns the group ordinal for
 * a vector of a document, adding it (via {@link #copy}) only if no other field of the document
 * holds an equal one. Callers with the same {@code (dimension, encoding)} share a group, so an
 * identical vector across fields of a document is stored a single time. Vectors must be added
 * document by document: a vector is only compared with those added since the document changed.
 *
 * <p>Not thread-safe; a group is confined to the writer that created it.
 *
 * @lucene.experimental
 */
abstract sealed class DedupGroup<T> implements Accountable
    permits DedupFlushContext.ByteGroup,
        DedupFlushContext.FloatGroup,
        DedupFlushContext.Float16Group,
        DedupMergeContext.DedupMergeGroup {

  private final List<T> vectors;

  private final IntArrayList docIDs; // the document of each vector, to sort them by doc on flush

  private final ObjectCursor<T> current; // reuse from addUnique

  private int lastDocID; // the document whose vectors were added last

  private int lastDocFirstOrd; // the group ordinal of its first vector

  DedupGroup() {
    this.vectors = new ArrayList<>();
    this.docIDs = new IntArrayList();
    this.current = new ObjectCursor<>();
    this.lastDocID = -1;
    this.lastDocFirstOrd = 0;
  }

  abstract boolean equals(T vector, T other) throws IOException;

  abstract T copy(T vector);

  abstract byte[] serialize(int ord) throws IOException;

  int numVectors() {
    return vectors.size();
  }

  T get(int ord) {
    return vectors.get(ord);
  }

  ObjectCursor<T> addUnique(int docID, T vectorValue) throws IOException {
    if (docID != lastDocID) {
      lastDocID = docID;
      lastDocFirstOrd = vectors.size();
    }
    int groupOrd = lastDocFirstOrd; // only compare with the vectors of the document
    while (groupOrd < vectors.size() && equals(vectorValue, vectors.get(groupOrd)) == false) {
      groupOrd++;
    }
    if (groupOrd == vectors.size()) { // not held by another field of the document
      vectors.add(copy(vectorValue)); // only for unique vectors
      docIDs.add(docID);
    }
    current.index = groupOrd;
    current.value = vectors.get(groupOrd);
    return current;
  }

  /**
   * The group ordinals sorted by the document of their vector (mapped through {@code sortMap} if
   * not null), the vectors of a document keeping their order.
   */
  int[] ordsByDoc(Sorter.DocMap sortMap) {
    int[] ords = new int[vectors.size()];
    for (int ord = 0; ord < ords.length; ord++) {
      ords[ord] = ord;
    }
    new InPlaceMergeSorter() {
      @Override
      protected int compare(int i, int j) {
        return Integer.compare(doc(ords[i]), doc(ords[j]));
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = ords[i];
        ords[i] = ords[j];
        ords[j] = tmp;
      }

      private int doc(int ord) {
        int docID = docIDs.get(ord);
        return sortMap == null ? docID : sortMap.oldToNew(docID);
      }
    }.sort(0, ords.length);
    return ords;
  }

  @Override
  public long ramBytesUsed() {
    return docIDs.ramBytesUsed();
  }
}
