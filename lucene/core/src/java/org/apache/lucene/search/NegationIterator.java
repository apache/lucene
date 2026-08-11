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

/**
 * Inverts any excluded {@link DocIdSetIterator} or {@link TwoPhaseIterator} so that it matches all
 * documents NOT matched by the wrapped excluded clause.
 *
 * <p>Overrides {@link #docIDRunEnd()} to efficiently expose NO blocks (spans of documents not
 * reached by the excluded iterator) as long runs, enabling {@link DenseConjunctionBulkScorer} to
 * use its {@code collectRange} shortcut for those spans. Overrides {@link #intoBitSet} to bulk-mark
 * excluded documents and subtract them, leveraging SIMD acceleration when the excluded clause
 * supports it (e.g., numeric range queries using the Panama vector API).
 *
 * <p>Designed for use in {@link DenseConjunctionBulkScorer} as a replacement for {@link
 * ReqExclBulkScorer} in dense FILTER + MUST_NOT scenarios.
 */
final class NegationIterator extends DocIdSetIterator {

  private final DocIdSetIterator exclApprox;
  private final TwoPhaseIterator exclTwoPhase;
  private final int maxDoc;
  private final FixedBitSet scratch;
  private int doc = -1;

  NegationIterator(DocIdSetIterator excl, int maxDoc) {
    this.exclApprox = excl;
    this.exclTwoPhase = null;
    this.maxDoc = maxDoc;
    this.scratch = new FixedBitSet(DenseConjunctionBulkScorer.WINDOW_SIZE);
  }

  NegationIterator(TwoPhaseIterator excl, int maxDoc) {
    this.exclApprox = excl.approximation();
    this.exclTwoPhase = excl;
    this.maxDoc = maxDoc;
    this.scratch = new FixedBitSet(DenseConjunctionBulkScorer.WINDOW_SIZE);
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public long cost() {
    // Matches nearly all documents; sort last in DenseConjunctionBulkScorer so cheaper
    // clauses are used as the lead.
    return maxDoc;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    doc = target;
    while (doc < maxDoc) {
      int exclDoc = exclApprox.docID();
      if (exclDoc < doc) {
        exclDoc = exclApprox.advance(doc);
      }
      if (exclDoc != doc) {
        // Excluded iterator is not at this doc: doc is not excluded.
        return doc;
      }
      // Excluded iterator is at this doc: check whether it is actually excluded.
      if (exclTwoPhase == null) {
        // Approximation is exact: skip the entire excluded run.
        int runEnd = exclApprox.docIDRunEnd();
        exclApprox.advance(runEnd);
        doc = runEnd;
      } else if (exclTwoPhase.matches()) {
        // Confirmed excluded: skip past this run (YES blocks jump to the block boundary).
        int runEnd = Math.max(doc + 1, exclTwoPhase.docIDRunEnd());
        exclApprox.advance(runEnd);
        doc = runEnd;
      } else {
        // False positive from the approximation: doc is NOT excluded.
        return doc;
      }
    }
    return doc = NO_MORE_DOCS;
  }

  @Override
  public int docIDRunEnd() throws IOException {
    int exclDoc = exclApprox.docID();
    if (exclDoc > doc) {
      // NO block: all documents in [doc, exclDoc) are guaranteed non-excluded.
      return exclDoc;
    }
    return doc + 1;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // Start with all docs in [doc, upTo) as candidates (negation matches everything).
    bitSet.set(Math.max(0, doc - offset), upTo - offset);

    // Fill scratch with the excluded documents. For numeric range queries this calls
    // rangeIntoBitSet which uses SIMD via the Panama vector API.
    assert scratch.scanIsEmpty() : "scratch must be clean before use";
    if (exclApprox.docID() < upTo) {
      if (exclTwoPhase != null) {
        exclTwoPhase.intoBitSet(upTo, scratch, offset);
      } else {
        exclApprox.intoBitSet(upTo, scratch, offset);
      }
    }

    // Remove excluded documents from the candidate set.
    bitSet.andNot(scratch);
    scratch.clear(0, upTo - offset);
    doc = upTo;
  }
}
