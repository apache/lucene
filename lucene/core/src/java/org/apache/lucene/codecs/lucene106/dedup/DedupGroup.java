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
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.internal.hppc.ObjectCursor;
import org.apache.lucene.util.Accountable;

/**
 * Interns vectors so that each distinct value is stored once. {@link #addUnique} returns the group
 * ordinal for a vector, adding it (via {@link #copy}) only if not already present. Callers with the
 * same {@code (dimension, encoding)} share a group, so an identical vector across fields is stored
 * a single time.
 *
 * <p>Not thread-safe; a group is confined to the writer that created it.
 *
 * @lucene.experimental
 */
abstract sealed class DedupGroup<T> implements Accountable
    permits DedupFlushContext.ByteGroup,
        DedupFlushContext.FloatGroup,
        DedupMergeContext.DedupMergeGroup {

  private static final int ORD_NOT_FOUND = -1;

  private final LongIntHashMap hashToOrdHint;
  private final List<T> vectors;

  private final ObjectCursor<T> current; // reuse from addUnique

  DedupGroup() {
    this.hashToOrdHint = new LongIntHashMap();
    this.vectors = new ArrayList<>();
    this.current = new ObjectCursor<>();
  }

  abstract long hash(T vector) throws IOException;

  abstract boolean equals(T vector, T other) throws IOException;

  abstract T copy(T vector);

  abstract byte[] serialize(int ord) throws IOException;

  int size() {
    return vectors.size();
  }

  T get(int ord) {
    return vectors.get(ord);
  }

  ObjectCursor<T> addUnique(T vectorValue) throws IOException {
    final int groupOrd;
    final T ownedVector;
    for (long hash = hash(vectorValue); ; hash++) { // linear probing
      int ordHint = hashToOrdHint.getOrDefault(hash, ORD_NOT_FOUND);
      if (ordHint == ORD_NOT_FOUND) {
        groupOrd = vectors.size();
        ownedVector = copy(vectorValue); // only for unique vectors
        hashToOrdHint.put(hash, groupOrd);
        vectors.add(ownedVector);
        break;
      } else {
        T other = vectors.get(ordHint);
        if (equals(vectorValue, other)) {
          groupOrd = ordHint;
          ownedVector = other;
          break;
        }
        // else continue probing
      }
    }
    current.index = groupOrd;
    current.value = ownedVector;
    return current;
  }

  @Override
  public long ramBytesUsed() {
    return hashToOrdHint.ramBytesUsed();
  }
}
