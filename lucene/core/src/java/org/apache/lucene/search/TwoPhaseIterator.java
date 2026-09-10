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
import java.util.Objects;
import org.apache.lucene.util.FixedBitSet;

/**
 * Returned by {@link Scorer#twoPhaseIterator()} to expose an approximation of a {@link
 * DocIdSetIterator}. When the {@link #approximation()}'s {@link DocIdSetIterator#nextDoc()} or
 * {@link DocIdSetIterator#advance(int)} return, {@link #matches()} needs to be checked in order to
 * know whether the returned doc ID actually matches.
 *
 * @lucene.experimental
 */
public abstract class TwoPhaseIterator {

  protected final DocIdSetIterator approximation;

  /** Takes the approximation to be returned by {@link #approximation}. Not null. */
  protected TwoPhaseIterator(DocIdSetIterator approximation) {
    this.approximation = Objects.requireNonNull(approximation);
  }

  /** Return a {@link DocIdSetIterator} view of the provided {@link TwoPhaseIterator}. */
  public static DocIdSetIterator asDocIdSetIterator(TwoPhaseIterator twoPhaseIterator) {
    return new TwoPhaseIteratorAsDocIdSetIterator(twoPhaseIterator);
  }

  /**
   * If the given {@link DocIdSetIterator} has been created with {@link #asDocIdSetIterator}, then
   * this will return the wrapped {@link TwoPhaseIterator}. Otherwise this returns {@code null}.
   */
  public static TwoPhaseIterator unwrap(DocIdSetIterator iterator) {
    if (iterator instanceof TwoPhaseIteratorAsDocIdSetIterator tpi) {
      return tpi.twoPhaseIterator;
    } else {
      return null;
    }
  }

  private static class TwoPhaseIteratorAsDocIdSetIterator extends FilterDocIdSetIterator {

    final TwoPhaseIterator twoPhaseIterator;

    TwoPhaseIteratorAsDocIdSetIterator(TwoPhaseIterator twoPhaseIterator) {
      super(twoPhaseIterator.approximation());
      this.twoPhaseIterator = twoPhaseIterator;
    }

    @Override
    public int nextDoc() throws IOException {
      return doNext(in.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
      return doNext(in.advance(target));
    }

    private int doNext(int doc) throws IOException {
      for (; ; doc = in.nextDoc()) {
        if (doc == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        } else if (twoPhaseIterator.matches()) {
          return doc;
        }
      }
    }
  }

  /**
   * Return an approximation. The returned {@link DocIdSetIterator} is a superset of the matching
   * documents, and each match needs to be confirmed with {@link #matches()} in order to know
   * whether it matches or not.
   */
  public DocIdSetIterator approximation() {
    return approximation;
  }

  /**
   * Return whether the current doc ID that {@link #approximation()} is on matches. This method
   * should only be called when the iterator is positioned -- ie. not when {@link
   * DocIdSetIterator#docID()} is {@code -1} or {@link DocIdSetIterator#NO_MORE_DOCS} -- and at most
   * once.
   */
  public abstract boolean matches() throws IOException;

  /**
   * An estimate of the expected cost to determine that a single document {@link #matches()}. This
   * can be called before iterating the documents of {@link #approximation()}. Returns an expected
   * cost in number of simple operations like addition, multiplication, comparing two numbers and
   * indexing an array. The returned value must be positive.
   */
  public abstract float matchCost();

  /**
   * Returns the end of the run of consecutive doc IDs that match this {@link TwoPhaseIterator} and
   * that contains the current doc ID of the approximation, that is: one plus the last doc ID of the
   * run.
   *
   * <p><b>Note</b>: It is illegal to call this method when the approximation is exhausted or not
   * positioned.
   *
   * <p>The default implementation is conservative and returns the current doc ID of the
   * approximation, i.e. it reports no matching run. Unlike a positioned {@link DocIdSetIterator} --
   * whose current doc is by definition a match -- a positioned {@link TwoPhaseIterator} has not yet
   * confirmed that the current doc matches, so the default cannot claim it as part of a matching
   * run without a (potentially side-effecting) {@link #matches()} call. Callers must therefore
   * treat a return value equal to {@code approximation().docID()} as "no known matching run" and
   * fall back to per-doc {@link #matches()} confirmation. Implementations that can cheaply prove
   * that a run of docs matches -- e.g. a doc-values skipper reporting a fully-matching block --
   * should override this to return the real run end.
   */
  public int docIDRunEnd() throws IOException {
    return approximation().docID();
  }

  /**
   * Load the doc IDs that both belong to the {@link #approximation()} and {@link #matches() match},
   * and are in {@code [approximation().docID(), upTo)}, into {@code bitSet}, the document whose ID
   * is {@code i} being stored at bit {@code i - offset}. Upon return the {@link #approximation()}
   * is positioned on the first doc that is {@code >= upTo}, mirroring {@link
   * DocIdSetIterator#intoBitSet}.
   *
   * <p>The default implementation walks the {@link #approximation()} and confirms each doc with
   * {@link #matches()} — i.e. it is functionally identical to the leap-frog evaluation, just
   * writing into a bit set. Implementations that can confirm matches in bulk — for instance a
   * vectorized scan over a columnar block — should override it; the bit set window is then
   * collected in one shot rather than one doc at a time.
   */
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    DocIdSetIterator approximation = approximation();
    for (int doc = approximation.docID(); doc < upTo; doc = approximation.nextDoc()) {
      if (matches()) {
        bitSet.set(doc - offset);
      }
    }
  }

  /**
   * Confirms {@link #matches()} only for the doc IDs whose bit is set in {@code bitSet} (the
   * document whose ID is {@code i} being stored at bit {@code i - offset}), clearing bits for docs
   * that turn out not to match. Upon return {@link #approximation()} is positioned on the first doc
   * that is {@code >= upTo}, mirroring {@link #intoBitSet}.
   *
   * <p>Prefer this over {@link #intoBitSet} followed by a separate intersection when {@code bitSet}
   * already holds a candidate set narrowed down by other clauses of a conjunction: it avoids
   * confirming matches for docs that are already known not to be candidates.
   *
   * <p>The default implementation walks the set bits of {@code bitSet} one at a time, advancing
   * {@link #approximation()} to each and calling {@link #matches()}. Implementations that can
   * classify a whole span of a sparse candidate set in bulk -- for instance a block-based skip
   * index that can tell that an entire span is fully matching or fully non-matching without
   * visiting doc values -- should override this.
   */
  public void applyMask(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    DocIdSetIterator approximation = approximation();
    int upperBound = Math.min(bitSet.length(), upTo - offset);
    for (int i = bitSet.nextSetBit(0);
        i != DocIdSetIterator.NO_MORE_DOCS && i < upperBound;
        i = i + 1 >= bitSet.length() ? DocIdSetIterator.NO_MORE_DOCS : bitSet.nextSetBit(i + 1)) {
      int doc = offset + i;
      int approxDoc = approximation.docID();
      if (approxDoc < doc) {
        approxDoc = approximation.advance(doc);
      }
      if (approxDoc != doc || matches() == false) {
        bitSet.clear(i);
      }
    }
    if (approximation.docID() < upTo) {
      approximation.advance(upTo);
    }
  }
}
