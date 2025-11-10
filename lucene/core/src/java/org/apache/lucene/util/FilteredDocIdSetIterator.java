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

import java.util.function.IntPredicate;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * {@link DocIdSetIterator} that returns documents matching a predicate by scanning all document
 * positions.
 *
 * <p>This generic iterator can be used to iterate either live or deleted documents based on a
 * predicate function. Iteration has O(maxDoc) complexity since it must scan all document positions.
 *
 * @lucene.internal
 */
final class FilteredDocIdSetIterator extends DocIdSetIterator {
  private final int maxDoc;
  private final int cost;
  private final IntPredicate predicate;
  private int doc = -1;

  /**
   * Creates a filtered iterator over documents.
   *
   * @param maxDoc the maximum document ID (exclusive)
   * @param cost the expected number of documents to return
   * @param predicate predicate to test each document ID; returns {@code true} if document should be
   *     included in iteration
   */
  FilteredDocIdSetIterator(int maxDoc, int cost, IntPredicate predicate) {
    this.maxDoc = maxDoc;
    this.cost = cost;
    this.predicate = predicate;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) {
    if (target >= maxDoc) {
      doc = NO_MORE_DOCS;
      return doc;
    }

    doc = target;
    while (doc < maxDoc) {
      if (predicate.test(doc)) {
        return doc;
      }
      doc++;
    }
    doc = NO_MORE_DOCS;
    return doc;
  }

  @Override
  public long cost() {
    return cost;
  }
}
