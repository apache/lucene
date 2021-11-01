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
package org.apache.lucene.util;

import org.apache.lucene.search.DocIdSet;

/**
 * A {@link DocIdSetProducer} produces {@link BitDocIdSet} or {@link RangeDocIdSet} depends on
 * whether a set of docIds collected is strictly sequential(contiguous). Strictly sequential :
 * {4,5,6,7,8}. Not sequential : {4,5,7,8}, missing docId 6.
 */
public class DocIdSetProducer {

  private FixedBitSet set;
  private int cardinality = 0;
  private int lastDocId = -1;
  private int minDoc = -1;
  private final int size;

  public DocIdSetProducer(int size) {
    this.size = size;
  }

  public void add(int docID) {
    if (docID <= lastDocId) {
      throw new IllegalArgumentException(
          "Out of order doc ids: last=" + lastDocId + ", next=" + docID);
    }
    if (minDoc == -1) {
      minDoc = cardinality = docID;
    }
    if (set != null) {
      set.set(docID);
    } else if (docID != cardinality) {
      set = new FixedBitSet(size);
      set.set(minDoc, cardinality);
      set.set(docID);
    }
    lastDocId = docID;
    cardinality++;
  }

  public DocIdSet get() {
    if (lastDocId == -1) return DocIdSet.EMPTY;
    return set == null ? new RangeDocIdSet(minDoc, cardinality) : new BitDocIdSet(set, cardinality);
  }
}
