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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;

/** Position of a term in a document that takes into account the term offset within the phrase. */
final class PhrasePositions {
  int position; // position in doc
  int count; // remaining pos in this doc
  int offset; // position in phrase
  final int ord; // unique across all PhrasePositions instances
  final PostingsEnum postings; // stream of docs & positions
  PhrasePositions next; // used to make lists
  int rptGroup = -1; // >=0 indicates that this is a repeating PP
  int rptInd; // index in the rptGroup
  final Term[] terms; // for repetitions initialization
  int freq; // cached frequency for the current document

  // Only tracked when offsets are being captured for match reporting (see
  // SloppyPhraseMatcher#captureLeadMatch); left at their default elsewhere to avoid the cost of
  // calling startOffset()/endOffset() during plain scoring, where they may not even be available.
  //
  // `startOffset`/`endOffset` always reflect the current `position`. `priorPosition`/
  // `priorStartOffset`/`priorEndOffset` remember the previous values, from just before the most
  // recent call to nextPosition(). They exist because SloppyPhraseMatcher#advanceRpts may need to
  // forcibly advance a *different* repeating PhrasePositions than the one actively being driven,
  // to resolve a position collision between the two. That forced advance overwrites the
  // sibling's position/startOffset/endOffset even though its *previous* values are what was
  // actually part of the match being reported; the prior* fields preserve those so they can
  // still be recovered. `bumped`, maintained by the caller, marks whether this instance's
  // current position/offsets came from such a forced advance (so the prior values, not the
  // current ones, should be reported for the in-progress match).
  int startOffset = -1;
  int endOffset = -1;
  int priorPosition;
  int priorStartOffset = -1;
  int priorEndOffset = -1;
  boolean bumped;

  // true when SloppyPhraseMatcher will be reporting match offsets (i.e. captureLeadMatch); false
  // during plain scoring, where startOffset()/endOffset() may not even be available on `postings`.
  private final boolean captureOffsets;

  PhrasePositions(PostingsEnum postings, int o, int ord, Term[] terms, boolean captureOffsets) {
    this.postings = postings;
    offset = o;
    this.ord = ord;
    this.terms = terms;
    this.captureOffsets = captureOffsets;
  }

  void firstPosition() throws IOException {
    count = freq; // use cached frequency
    nextPosition();
  }

  /**
   * Go to next location of this term current document, and set <code>position</code> as <code>
   * location - offset</code>, so that a matching exact phrase is easily identified when all
   * PhrasePositions have exactly the same <code>position</code>.
   */
  boolean nextPosition() throws IOException {
    if (count-- > 0) { // read subsequent pos's
      int newPosition = postings.nextPosition() - offset;
      if (captureOffsets) {
        priorPosition = position;
        priorStartOffset = startOffset;
        priorEndOffset = endOffset;
        startOffset = postings.startOffset();
        endOffset = postings.endOffset();
      }
      position = newPosition;
      return true;
    } else {
      return false;
    }
  }

  /** for debug purposes */
  @Override
  public String toString() {
    String s = "o:" + offset + " p:" + position + " c:" + count;
    if (rptGroup >= 0) {
      s += " rpt:" + rptGroup + ",i" + rptInd;
    }
    return s;
  }
}
