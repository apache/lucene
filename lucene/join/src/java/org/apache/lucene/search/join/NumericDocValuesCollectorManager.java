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
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

class NumericDocValuesCollectorManager<
        C extends NumericDocValuesCollectorManager.NumericDocValuesCollector>
    implements CollectorManager<C, LongHashSet> {
  private final Supplier<C> collectorSupplier;

  NumericDocValuesCollectorManager(Supplier<C> collectorSupplier) {
    this.collectorSupplier = collectorSupplier;
  }

  @Override
  public C newCollector() throws IOException {
    return collectorSupplier.get();
  }

  @Override
  public LongHashSet reduce(Collection<C> collectors) throws IOException {
    LongHashSet result = new LongHashSet();
    for (C collector : collectors) {
      result.addAll(collector.joinValues);
    }
    return result;
  }

  abstract static class NumericDocValuesCollector extends SimpleCollector {
    final boolean needsScore;
    final String fromField;
    final LongHashSet joinValues = new LongHashSet();
    final JoinUtil.LongFloatProcedure scoreAggregator;
    Scorable scorer;

    NumericDocValuesCollector(
        boolean needsScore, String fromField, JoinUtil.LongFloatProcedure scoreAggregator) {
      this.needsScore = needsScore;
      this.fromField = fromField;
      this.scoreAggregator = scoreAggregator;
    }

    @Override
    public ScoreMode scoreMode() {
      return needsScore
          ? org.apache.lucene.search.ScoreMode.COMPLETE
          : org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }

    static class MV extends NumericDocValuesCollector {
      SortedNumericDocValues sortedNumericDocValues;

      MV(boolean needsScore, String fromField, JoinUtil.LongFloatProcedure scoreAggregator) {
        super(needsScore, fromField, scoreAggregator);
      }

      @Override
      public void collect(int doc) throws IOException {
        if (sortedNumericDocValues.advanceExact(doc)) {
          for (int i = 0, count = sortedNumericDocValues.docValueCount(); i < count; i++) {
            long value = sortedNumericDocValues.nextValue();
            joinValues.add(value);
            if (needsScore) {
              scoreAggregator.apply(value, scorer.score());
            }
          }
        }
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        sortedNumericDocValues = DocValues.getSortedNumeric(context.reader(), fromField);
      }
    }

    static class SV extends NumericDocValuesCollector {
      NumericDocValues numericDocValues;
      private int lastDocID = -1;

      SV(boolean needsScore, String fromField, JoinUtil.LongFloatProcedure scoreAggregator) {
        super(needsScore, fromField, scoreAggregator);
      }

      private boolean docsInOrder(int docID) {
        if (docID < lastDocID) {
          throw new AssertionError(
              "docs out of order: lastDocID=" + lastDocID + " vs docID=" + docID);
        }
        lastDocID = docID;
        return true;
      }

      @Override
      public void collect(int doc) throws IOException {
        assert docsInOrder(doc);
        long value = 0;
        if (numericDocValues.advanceExact(doc)) {
          value = numericDocValues.longValue();
        }
        joinValues.add(value);
        if (needsScore) {
          scoreAggregator.apply(value, scorer.score());
        }
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        numericDocValues = DocValues.getNumeric(context.reader(), fromField);
        lastDocID = -1;
      }
    }
  }
}
