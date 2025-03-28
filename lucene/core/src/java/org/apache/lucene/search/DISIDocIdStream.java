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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.util.FixedBitSet;

final class DISIDocIdStream extends DocIdStream {

  private final DocIdSetIterator iterator;
  private final int max;
  private final FixedBitSet spare;

  DISIDocIdStream(DocIdSetIterator iterator, int max, FixedBitSet spare) {
    if (max - iterator.docID() > spare.length()) {
      throw new IllegalArgumentException("Bit set is too small to hold all potential matches");
    }
    this.iterator = iterator;
    this.max = max;
    this.spare = spare;
  }

  @Override
  public boolean mayHaveRemaining() {
    return iterator.docID() < max;
  }

  @Override
  public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
    // If there are no live docs to apply, loading matching docs into a bit set and then iterating
    // bits is unlikely to beat iterating the iterator directly.
    upTo = Math.min(upTo, max);
    for (int doc = iterator.docID(); doc < upTo; doc = iterator.nextDoc()) {
      consumer.accept(doc);
    }
  }

  @Override
  public int count(int upTo) throws IOException {
    if (iterator.docID() >= upTo) {
      return 0;
    }
    // If the collector is just interested in the count, loading in a bit set and counting bits is
    // often faster than incrementing a counter on every call to nextDoc().
    assert spare.scanIsEmpty();
    upTo = Math.min(upTo, max);
    int offset = iterator.docID();
    iterator.intoBitSet(upTo, spare, offset);
    int count = spare.cardinality(0, upTo - offset);
    spare.clear(0, upTo - offset);
    assert spare.scanIsEmpty();
    return count;
  }
}
