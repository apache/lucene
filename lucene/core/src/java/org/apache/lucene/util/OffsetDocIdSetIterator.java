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

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Iterator that adds an offset to all doc IDs from the underlying iterator, converting
 * partition-relative indices back to absolute doc IDs.
 *
 * <p>This is used for partition-aware queries where the underlying iterator returns relative
 * indices (0 to partitionSize-1), but callers expect absolute doc IDs (minDocId to maxDocId-1).
 *
 * @lucene.internal
 */
final class OffsetDocIdSetIterator extends DocIdSetIterator {
  private final DocIdSetIterator delegate;
  private final int offset;

  /**
   * Creates an offset wrapper around a DocIdSetIterator.
   *
   * @param delegate the underlying iterator returning partition-relative doc IDs
   * @param offset the value to add to convert relative indices to absolute doc IDs (typically
   *     minDocId)
   */
  OffsetDocIdSetIterator(DocIdSetIterator delegate, int offset) {
    this.delegate = delegate;
    this.offset = offset;
  }

  @Override
  public int docID() {
    int doc = delegate.docID();
    return doc == NO_MORE_DOCS ? NO_MORE_DOCS : doc + offset;
  }

  @Override
  public int nextDoc() throws IOException {
    int doc = delegate.nextDoc();
    return doc == NO_MORE_DOCS ? NO_MORE_DOCS : doc + offset;
  }

  @Override
  public int advance(int target) throws IOException {
    // Convert target from absolute to partition-relative, advance, then convert back
    int relativeTarget = target - offset;
    int doc = delegate.advance(Math.max(0, relativeTarget));
    return doc == NO_MORE_DOCS ? NO_MORE_DOCS : doc + offset;
  }

  @Override
  public long cost() {
    return delegate.cost();
  }
}
