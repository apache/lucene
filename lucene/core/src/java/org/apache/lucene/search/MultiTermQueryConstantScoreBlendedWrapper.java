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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * This class provides the functionality behind {@link
 * MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE}. It maintains a boolean query-like approach over a
 * limited number of the most costly terms while rewriting the remaining terms into a filter bitset.
 */
final class MultiTermQueryConstantScoreBlendedWrapper<Q extends MultiTermQuery>
    extends AbstractMultiTermQueryConstantScoreWrapper<Q> {
  // postings lists under this threshold will always be "pre-processed" into a bitset
  private static final int POSTINGS_PRE_PROCESS_THRESHOLD = 512;

  MultiTermQueryConstantScoreBlendedWrapper(Q query) {
    super(query);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new RewritingWeight(query, boost, scoreMode, searcher) {

      @Override
      protected WeightOrDocIdSetIterator rewriteInner(
          LeafReaderContext context,
          int fieldDocCount,
          Terms terms,
          TermsEnum termsEnum,
          List<TermAndState> collectedTerms)
          throws IOException {
        DocIdSetBuilder otherTerms = new DocIdSetBuilder(context.reader().maxDoc(), terms);
        PriorityQueue<PostingsEnum> highFrequencyTerms =
            new PriorityQueue<>(collectedTerms.size()) {
              @Override
              protected boolean lessThan(PostingsEnum a, PostingsEnum b) {
                return a.cost() < b.cost();
              }
            };

        // Handle the already-collected terms:
        PostingsEnum reuse = null;
        if (collectedTerms.isEmpty() == false) {
          TermsEnum termsEnum2 = terms.iterator();
          for (TermAndState t : collectedTerms) {
            termsEnum2.seekExact(t.term, t.state);
            reuse = termsEnum2.postings(reuse, PostingsEnum.NONE);
            if (t.docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
              otherTerms.add(reuse);
            } else {
              highFrequencyTerms.add(reuse);
              reuse = null; // can't reuse since we haven't processed the postings
            }
          }
        }

        // Then collect remaining terms:
        do {
          reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
          // If a term contains all docs with a value for the specified field, we can discard the
          // other terms and just use the dense term's postings:
          int docFreq = termsEnum.docFreq();
          if (fieldDocCount == docFreq) {
            TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(
                termsEnum.termState(), context.ord, docFreq, termsEnum.totalTermFreq());
            Query q =
                new ConstantScoreQuery(
                    new TermQuery(new Term(query.field, termsEnum.term()), termStates));
            Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
            return new WeightOrDocIdSetIterator(weight);
          }

          if (docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
            otherTerms.add(reuse);
          } else {
            PostingsEnum dropped = highFrequencyTerms.insertWithOverflow(reuse);
            if (dropped != null) {
              otherTerms.add(dropped);
            }
            // Reuse the postings that drop out of the PQ. Note that `dropped` will be null here
            // if nothing is evicted, meaning we will _not_ reuse any postings (which is intentional
            // since we can't reuse postings that are in the PQ).
            reuse = dropped;
          }
        } while (termsEnum.next() != null);

        DisiPriorityQueue subs = new DisiPriorityQueue(highFrequencyTerms.size() + 1);
        for (DocIdSetIterator disi : highFrequencyTerms) {
          Scorer s = wrapWithDummyScorer(this, disi);
          subs.add(new DisiWrapper(s));
        }
        Scorer s = wrapWithDummyScorer(this, otherTerms.build().iterator());
        subs.add(new DisiWrapper(s));

        return new WeightOrDocIdSetIterator(new DisjunctionDISIApproximation(subs));
      }
    };
  }

  /**
   * Wraps a DISI with a "dummy" scorer so we can easily use {@link DisiWrapper} and {@link
   * DisjunctionDISIApproximation} as-is. This is really just a convenient vehicle to get the DISI
   * into the priority queue used by {@link DisjunctionDISIApproximation}. The {@link Scorer}
   * ultimately provided by the weight provides a constant boost and reflects the actual score mode.
   */
  private static Scorer wrapWithDummyScorer(Weight weight, DocIdSetIterator disi) {
    // The score and score mode do not actually matter here, except that using TOP_SCORES results
    // in another wrapper object getting created around the disi, so we try to avoid that:
    return new ConstantScoreScorer(weight, 1f, ScoreMode.COMPLETE_NO_SCORES, disi);
  }
}
