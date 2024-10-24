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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

class TermsWithScoreCollectorManager<
        C extends TermsWithScoreCollectorManager.TermsWithScoreCollector<DV, C>, DV>
    implements CollectorManager<C, GenericTermsResult> {
  private final Supplier<C> collectorSupplier;

  public TermsWithScoreCollectorManager(Supplier<C> collectorSupplier) {
    this.collectorSupplier = collectorSupplier;
  }

  @Override
  public C newCollector() {
    return collectorSupplier.get();
  }

  @Override
  public GenericTermsResult reduce(Collection<C> collectors) throws IOException {
    C reducedCollector = collectorSupplier.get();
    for (C c : collectors) {
      reducedCollector.merge(c);
    }
    return reducedCollector;
  }

  abstract static class TermsWithScoreCollector<DV, C extends TermsWithScoreCollector<DV, C>>
      extends DocValuesTermsCollector<DV> implements GenericTermsResult {

    private static final int INITIAL_ARRAY_SIZE = 0;

    final BytesRefHash collectedTerms = new BytesRefHash();
    final ScoreMode scoreMode;

    Scorable scorer;
    float[] scoreSums = new float[INITIAL_ARRAY_SIZE];

    TermsWithScoreCollector(Function<DV> docValuesCall, ScoreMode scoreMode) {
      super(docValuesCall);
      this.scoreMode = scoreMode;
      if (scoreMode == ScoreMode.Min) {
        Arrays.fill(scoreSums, Float.POSITIVE_INFINITY);
      } else if (scoreMode == ScoreMode.Max) {
        Arrays.fill(scoreSums, Float.NEGATIVE_INFINITY);
      }
    }

    @Override
    public BytesRefHash getCollectedTerms() {
      return collectedTerms;
    }

    @Override
    public float[] getScoresPerTerm() {
      return scoreSums;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }

    public void merge(C collector) throws IOException {
      int[] ids = collector.collectedTerms.compact();
      BytesRef scratch = new BytesRef();
      for (int i = 0; i < collector.collectedTerms.size(); i++) {
        merge(collector, ids[i], scratch);
      }
    }

    void merge(C other, int id, BytesRef scratch) {
      // Most scoreModes can use this implementation. Avg needs the term count too.
      collectTerm(other.collectedTerms.get(id, scratch), other.scoreSums[id]);
    }
    ;

    abstract void collectTerm(BytesRef term, float score);

    /**
     * Chooses the right {@link TermsWithScoreCollector} implementation.
     *
     * @param field The field to collect terms for
     * @param multipleValuesPerDocument Whether the field to collect terms for has multiple values
     *     per document.
     * @return a {@link TermsWithScoreCollector} instance
     */
    static TermsWithScoreCollector<?, ?> create(
        String field, boolean multipleValuesPerDocument, ScoreMode scoreMode) {
      if (multipleValuesPerDocument) {
        if (Objects.requireNonNull(scoreMode) == ScoreMode.Avg) {
          return new MVAvg(sortedSetDocValues(field));
        }
        return new MV(sortedSetDocValues(field), scoreMode);
      } else {
        if (Objects.requireNonNull(scoreMode) == ScoreMode.Avg) {
          return new SVAvg(sortedDocValues(field));
        }
        return new SV(sortedDocValues(field), scoreMode);
      }
    }

    // impl that works with single value per document
    static class SV extends TermsWithScoreCollector<SortedDocValues, SV> {

      SV(Function<SortedDocValues> docValuesCall, ScoreMode scoreMode) {
        super(docValuesCall, scoreMode);
      }

      @Override
      public void collect(int doc) throws IOException {
        BytesRef value;
        if (docValues.advanceExact(doc)) {
          value = docValues.lookupOrd(docValues.ordValue());
        } else {
          value = new BytesRef(BytesRef.EMPTY_BYTES);
        }
        collectTerm(value, scorer.score());
      }

      @Override
      void collectTerm(BytesRef term, float score) {
        int ord = collectedTerms.add(term);
        if (ord < 0) {
          ord = -ord - 1;
        } else {
          if (ord >= scoreSums.length) {
            int begin = scoreSums.length;
            scoreSums = ArrayUtil.grow(scoreSums);
            if (scoreMode == ScoreMode.Min) {
              Arrays.fill(scoreSums, begin, scoreSums.length, Float.POSITIVE_INFINITY);
            } else if (scoreMode == ScoreMode.Max) {
              Arrays.fill(scoreSums, begin, scoreSums.length, Float.NEGATIVE_INFINITY);
            }
          }
        }

        float existing = scoreSums[ord];
        if (Float.compare(existing, 0.0f) == 0) {
          scoreSums[ord] = score;
        } else {
          switch (scoreMode) {
            case Total:
              scoreSums[ord] = scoreSums[ord] + score;
              break;
            case Min:
              if (score < existing) {
                scoreSums[ord] = score;
              }
              break;
            case Max:
              if (score > existing) {
                scoreSums[ord] = score;
              }
              break;
            case None:
            case Avg:
            default:
              throw new AssertionError("unexpected: " + scoreMode);
          }
        }
      }
    }

    // impl that works with multiple values per document
    static class MV extends TermsWithScoreCollector<SortedSetDocValues, MV> {

      MV(Function<SortedSetDocValues> docValuesCall, ScoreMode scoreMode) {
        super(docValuesCall, scoreMode);
      }

      @Override
      void collectTerm(BytesRef term, float score) {
        int termID = collectedTerms.add(term);
        if (termID < 0) {
          termID = -termID - 1;
        } else {
          if (termID >= scoreSums.length) {
            int begin = scoreSums.length;
            scoreSums = ArrayUtil.grow(scoreSums);
            if (scoreMode == ScoreMode.Min) {
              Arrays.fill(scoreSums, begin, scoreSums.length, Float.POSITIVE_INFINITY);
            } else if (scoreMode == ScoreMode.Max) {
              Arrays.fill(scoreSums, begin, scoreSums.length, Float.NEGATIVE_INFINITY);
            }
          }
        }

        switch (scoreMode) {
          case Total:
            scoreSums[termID] += score;
            break;
          case Min:
            scoreSums[termID] = Math.min(scoreSums[termID], score);
            break;
          case Max:
            scoreSums[termID] = Math.max(scoreSums[termID], score);
            break;
          case Avg:
          case None:
          default:
            throw new AssertionError("unexpected: " + scoreMode);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        if (docValues.advanceExact(doc)) {
          for (int i = 0; i < docValues.docValueCount(); i++) {
            collectTerm(docValues.lookupOrd(docValues.nextOrd()), scorer.score());
          }
        }
      }
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
      return org.apache.lucene.search.ScoreMode.COMPLETE;
    }

    static class SVAvg extends TermsWithScoreCollector<SortedDocValues, SVAvg> {

      int[] scoreCounts = new int[INITIAL_ARRAY_SIZE];

      SVAvg(Function<SortedDocValues> docValuesCall) {
        super(docValuesCall, ScoreMode.Avg);
      }

      @Override
      public void collect(int doc) throws IOException {
        BytesRef value;
        if (docValues.advanceExact(doc)) {
          value = docValues.lookupOrd(docValues.ordValue());
        } else {
          value = new BytesRef(BytesRef.EMPTY_BYTES);
        }
        this.collectTerm(value, scorer.score());
      }

      void collectTerm(BytesRef term, float score, int count) {
        int ord = collectedTerms.add(term);
        if (ord < 0) {
          ord = -ord - 1;
        } else {
          if (ord >= scoreSums.length) {
            scoreSums = ArrayUtil.grow(scoreSums);
            scoreCounts = ArrayUtil.grow(scoreCounts);
          }
        }

        float existing = scoreSums[ord];
        if (Float.compare(existing, 0.0f) == 0) {
          scoreSums[ord] = score;
          scoreCounts[ord] = count;
        } else {
          scoreSums[ord] = scoreSums[ord] + score;
          scoreCounts[ord] += count;
        }
      }

      @Override
      void merge(SVAvg other, int id, BytesRef scratch) {
        collectTerm(
            other.collectedTerms.get(id, scratch), other.scoreSums[id], other.scoreCounts[id]);
      }

      @Override
      void collectTerm(BytesRef term, float score) {
        collectTerm(term, score, 1);
      }

      @Override
      public float[] getScoresPerTerm() {
        if (scoreCounts != null) {
          for (int i = 0; i < scoreCounts.length; i++) {
            scoreSums[i] = scoreSums[i] / scoreCounts[i];
          }
          scoreCounts = null;
        }
        return scoreSums;
      }
    }

    static class MVAvg extends TermsWithScoreCollector<SortedSetDocValues, MVAvg> {

      int[] scoreCounts = new int[INITIAL_ARRAY_SIZE];

      MVAvg(Function<SortedSetDocValues> docValuesCall) {
        super(docValuesCall, ScoreMode.Avg);
      }

      @Override
      public void collect(int doc) throws IOException {
        if (docValues.advanceExact(doc)) {
          for (int i = 0; i < docValues.docValueCount(); i++) {
            BytesRef term = docValues.lookupOrd(docValues.nextOrd());
            collectTerm(term, scorer.score());
          }
        }
      }

      @Override
      void collectTerm(BytesRef term, float score) {
        collectTerm(term, score, 1);
      }

      void collectTerm(BytesRef term, float score, int count) {
        int termID = collectedTerms.add(term);
        if (termID < 0) {
          termID = -termID - 1;
        } else {
          if (termID >= scoreSums.length) {
            scoreSums = ArrayUtil.grow(scoreSums);
            scoreCounts = ArrayUtil.grow(scoreCounts);
          }
        }

        scoreSums[termID] += score;
        scoreCounts[termID] += count;
      }

      @Override
      void merge(MVAvg other, int id, BytesRef scratch) {
        collectTerm(
            other.collectedTerms.get(id, scratch), other.scoreSums[id], other.scoreCounts[id]);
      }

      @Override
      public float[] getScoresPerTerm() {
        if (scoreCounts != null) {
          for (int i = 0; i < scoreCounts.length; i++) {
            scoreSums[i] = scoreSums[i] / scoreCounts[i];
          }
          scoreCounts = null;
        }
        return scoreSums;
      }
    }
  }
}
