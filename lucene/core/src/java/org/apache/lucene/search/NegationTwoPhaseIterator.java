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
 * <p>Uses {@link DocIdSetIterator#all} as the approximation so that {@link
 * DocIdSetIterator#advance} is O(1) regardless of the excluded clause's structure. This makes the
 * iterator safe for use in a sliced context: each slice advances to its start in O(1) without
 * iterating through any excluded documents.
 *
 * <p>Overrides {@link #docIDRunEnd()} to expose NO blocks (spans where the excluded clause has no
 * documents), enabling {@link DenseConjunctionBulkScorer} to use its {@code collectRange} shortcut
 * for those spans. Overrides {@link #applyMask} to subtract excluded documents in bulk from the
 * candidate set, leveraging SIMD acceleration when the excluded clause supports it (e.g., numeric
 * range queries via the Panama vector API).
 *
 * <p>Designed for use in {@link DenseConjunctionBulkScorer} as a replacement for {@link
 * ReqExclBulkScorer} in dense FILTER + MUST_NOT scenarios.
 */
final class NegationTwoPhaseIterator extends TwoPhaseIterator {

  private final DocIdSetIterator exclApprox;
  private final TwoPhaseIterator exclTwoPhase;
  private final FixedBitSet scratch;

  NegationTwoPhaseIterator(DocIdSetIterator excl, int maxDoc) {
    super(DocIdSetIterator.all(maxDoc));
    this.exclApprox = excl;
    this.exclTwoPhase = null;
    this.scratch = new FixedBitSet(DenseConjunctionBulkScorer.WINDOW_SIZE);
  }

  NegationTwoPhaseIterator(TwoPhaseIterator excl, int maxDoc) {
    super(DocIdSetIterator.all(maxDoc));
    this.exclApprox = excl.approximation();
    this.exclTwoPhase = excl;
    this.scratch = new FixedBitSet(DenseConjunctionBulkScorer.WINDOW_SIZE);
  }

  @Override
  public boolean matches() throws IOException {
    int doc = approximation().docID();
    int exclDoc = exclApprox.docID();
    if (exclDoc < doc) {
      exclDoc = exclApprox.advance(doc);
    }
    if (exclDoc != doc) {
      return true; // excluded iterator is not at this doc: not excluded
    }
    if (exclTwoPhase == null) {
      return false; // plain DISI: doc is definitively excluded
    }
    return !exclTwoPhase.matches(); // two-phase: excluded only if confirmed
  }

  @Override
  public float matchCost() {
    return exclTwoPhase != null ? exclTwoPhase.matchCost() + 1f : 2f;
  }

  @Override
  public int docIDRunEnd() throws IOException {
    int doc = approximation().docID();
    int exclDoc = exclApprox.docID();
    if (exclDoc < doc) {
      // exclApprox is behind (e.g. left there by a prior matches() call): advance it so we can
      // check whether the excluded clause has any docs at or after the current position.
      exclDoc = exclApprox.advance(doc);
    }
    if (exclDoc > doc) {
      // NO block: all documents in [doc, exclDoc) are guaranteed non-excluded.
      return exclDoc;
    }
    return doc + 1;
  }

  /**
   * Bulk-fills {@code bitSet} with the doc IDs that match this iterator (all docs in the window
   * minus those excluded), using SIMD acceleration when the excluded clause supports it (e.g.
   * numeric range queries via the Panama vector API on MAYBE blocks).
   *
   * <p>This override is critical for correctness and performance when this iterator is the lead
   * clause in {@link DenseConjunctionBulkScorer}: without it the default {@link
   * TwoPhaseIterator#intoBitSet} falls back to a per-doc {@link #matches()} loop that bypasses the
   * SIMD path entirely.
   */
  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // Mark all docs in [approximation().docID(), upTo) as candidates (negation matches
    // everything).
    bitSet.set(Math.max(0, approximation().docID() - offset), upTo - offset);

    // Fill scratch with the excluded documents. For numeric range queries this calls
    // rangeIntoBitSet which uses SIMD via the Panama vector API.
    assert scratch.scanIsEmpty() : "scratch must be clean before use";
    if (exclApprox.docID() < offset) {
      exclApprox.advance(offset);
    }
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

    // Advance the approximation to upTo per the intoBitSet contract.
    if (approximation().docID() < upTo) {
      approximation().advance(upTo);
    }
  }

  /**
   * Removes excluded documents from {@code bitSet} in bulk. For numeric range queries the excluded
   * clause's {@link TwoPhaseIterator#intoBitSet} calls {@code rangeIntoBitSet}, enabling SIMD
   * acceleration via the Panama vector API on MAYBE blocks.
   */
  @Override
  public void applyMask(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    // Ensure the excluded approximation is positioned at or past the window start.
    if (exclApprox.docID() < offset) {
      exclApprox.advance(offset);
    }

    // Fill scratch with the excluded documents using the excluded clause's bulk intoBitSet.
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

    // Advance the approximation to upTo per the applyMask contract.
    if (approximation().docID() < upTo) {
      approximation().advance(upTo);
    }
  }
}
