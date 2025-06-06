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
import java.util.Comparator;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.PriorityQueue;

/** Util class for Scorer related methods */
class ScorerUtil {

  private static final Class<?> DEFAULT_IMPACTS_ENUM_CLASS =
      Lucene103PostingsFormat.getImpactsEnumImpl();
  private static final Class<?> DEFAULT_ACCEPT_DOCS_CLASS =
      new FixedBitSet(1).asReadOnlyBits().getClass();

  static long costWithMinShouldMatch(LongStream costs, int numScorers, int minShouldMatch) {
    // the idea here is the following: a boolean query c1,c2,...cn with minShouldMatch=m
    // could be rewritten to:
    // (c1 AND (c2..cn|msm=m-1)) OR (!c1 AND (c2..cn|msm=m))
    // if we assume that clauses come in ascending cost, then
    // the cost of the first part is the cost of c1 (because the cost of a conjunction is
    // the cost of the least costly clause)
    // the cost of the second part is the cost of finding m matches among the c2...cn
    // remaining clauses
    // since it is a disjunction overall, the total cost is the sum of the costs of these
    // two parts

    // If we recurse infinitely, we find out that the cost of a msm query is the sum of the
    // costs of the num_scorers - minShouldMatch + 1 least costly scorers
    final PriorityQueue<Long> pq =
        PriorityQueue.usingComparator(numScorers - minShouldMatch + 1, Comparator.reverseOrder());
    costs.forEach(pq::insertWithOverflow);
    return StreamSupport.stream(pq.spliterator(), false).mapToLong(Number::longValue).sum();
  }

  /**
   * Optimize a {@link DocIdSetIterator} for the case when it is likely implemented via an {@link
   * ImpactsEnum}. This return method only has 2 possible return types, which helps make sure that
   * calls to {@link DocIdSetIterator#nextDoc()} and {@link DocIdSetIterator#advance(int)} are
   * bimorphic at most and candidate for inlining.
   */
  static DocIdSetIterator likelyImpactsEnum(DocIdSetIterator it) {
    if (it.getClass() != DEFAULT_IMPACTS_ENUM_CLASS
        && it.getClass() != FilterDocIdSetIterator.class) {
      it = new FilterDocIdSetIterator(it);
    }
    return it;
  }

  /**
   * Optimize a {@link Scorable} for the case when it is likely implemented via a {@link
   * TermScorer}. This return method only has 2 possible return types, which helps make sure that
   * calls to {@link Scorable#score()} are bimorphic at most and candidate for inlining.
   */
  static Scorable likelyTermScorer(Scorable scorable) {
    if (scorable.getClass() != TermScorer.class && scorable.getClass() != FilterScorable.class) {
      scorable = new FilterScorable(scorable);
    }
    return scorable;
  }

  /**
   * Optimize {@link Bits} representing the set of accepted documents for the case when it is likely
   * implemented as live docs. This helps make calls to {@link Bits#get(int)} inlinable, which
   * in-turn helps speed up query evaluation. This is especially helpful as inlining will sometimes
   * enable auto-vectorizing shifts and masks that are done in {@link FixedBitSet#get(int)}.
   */
  static Bits likelyLiveDocs(Bits acceptDocs) {
    if (acceptDocs == null) {
      return acceptDocs;
    } else if (acceptDocs.getClass() == DEFAULT_ACCEPT_DOCS_CLASS) {
      return acceptDocs;
    } else {
      return new FilterBits(acceptDocs);
    }
  }

  private static class FilterBits implements Bits {

    private final Bits in;

    FilterBits(Bits in) {
      this.in = in;
    }

    @Override
    public boolean get(int index) {
      return in.get(index);
    }

    @Override
    public int length() {
      return in.length();
    }
  }

  /**
   * Filters competitive hits from the provided {@link DocAndScoreAccBuffer}.
   *
   * <p>This method removes documents from the buffer that cannot possibly have a score competitive
   * enough to exceed the minimum competitive score, given the maximum remaining score and the
   * number of scorers.
   */
  static void filterCompetitiveHits(
      DocAndScoreAccBuffer buffer,
      double maxRemainingScore,
      float minCompetitiveScore,
      int numScorers) {
    if ((float) MathUtil.sumUpperBound(maxRemainingScore, numScorers) >= minCompetitiveScore) {
      return;
    }

    int newSize = 0;
    for (int i = 0; i < buffer.size; ++i) {
      float maxPossibleScore =
          (float) MathUtil.sumUpperBound(buffer.scores[i] + maxRemainingScore, numScorers);
      if (maxPossibleScore >= minCompetitiveScore) {
        buffer.docs[newSize] = buffer.docs[i];
        buffer.scores[newSize] = buffer.scores[i];
        newSize++;
      }
    }
    buffer.size = newSize;
  }

  /**
   * Apply the provided {@link Scorable} as a required clause on the given {@link
   * DocAndScoreAccBuffer}. This filters out documents from the buffer that do not match, and adds
   * the scores of this {@link Scorable} to the scores.
   *
   * <p><b>NOTE</b>: The provided buffer must contain doc IDs in sorted order, with no duplicates.
   */
  static void applyRequiredClause(
      DocAndScoreAccBuffer buffer, DocIdSetIterator iterator, Scorable scorable)
      throws IOException {
    int intersectionSize = 0;
    int curDoc = iterator.docID();
    for (int i = 0; i < buffer.size; ++i) {
      int targetDoc = buffer.docs[i];
      if (curDoc < targetDoc) {
        curDoc = iterator.advance(targetDoc);
      }
      if (curDoc == targetDoc) {
        buffer.docs[intersectionSize] = targetDoc;
        buffer.scores[intersectionSize] = buffer.scores[i] + scorable.score();
        intersectionSize++;
      }
    }
    buffer.size = intersectionSize;
  }

  /**
   * Apply the provided {@link Scorable} as an optional clause on the given {@link
   * DocAndScoreAccBuffer}. This adds the scores of this {@link Scorer} to the existing scores.
   *
   * <p><b>NOTE</b>: The provided buffer must contain doc IDs in sorted order, with no duplicates.
   */
  static void applyOptionalClause(
      DocAndScoreAccBuffer buffer, DocIdSetIterator iterator, Scorable scorable)
      throws IOException {
    int curDoc = iterator.docID();
    for (int i = 0; i < buffer.size; ++i) {
      int targetDoc = buffer.docs[i];
      if (curDoc < targetDoc) {
        curDoc = iterator.advance(targetDoc);
      }
      if (curDoc == targetDoc) {
        buffer.scores[i] += scorable.score();
      }
    }
  }
}
