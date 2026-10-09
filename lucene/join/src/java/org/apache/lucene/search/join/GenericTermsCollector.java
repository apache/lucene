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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.join.DocValuesTermsCollector.Function;
import org.apache.lucene.search.join.TermsWithScoreCollector.MV;
import org.apache.lucene.search.join.TermsWithScoreCollector.SV;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

interface GenericTermsCollector extends Collector {

  BytesRefHash getCollectedTerms();

  float[] getScoresPerTerm();

  default int[] getTermCounts() {
    return null;
  }

  static GenericTermsCollector createCollectorMV(
      Function<SortedSetDocValues> mvFunction, ScoreMode mode) {

    switch (mode) {
      case None:
        return wrap(new TermsCollector.MV(mvFunction));
      case Avg:
        return new MV.Avg(mvFunction);
      case Max:
      case Min:
      case Total:
      default:
        return new MV(mvFunction, mode);
    }
  }

  static Function<SortedSetDocValues> verbose(
      PrintStream out, Function<SortedSetDocValues> mvFunction) {
    return (ctx) -> {
      final SortedSetDocValues target = mvFunction.apply(ctx);
      return new SortedSetDocValues() {

        @Override
        public int docID() {
          return target.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          int docID = target.nextDoc();
          out.println("\nnextDoc doc# " + docID);
          return docID;
        }

        @Override
        public int advance(int dest) throws IOException {
          int docID = target.advance(dest);
          out.println("\nadvance(" + dest + ") -> doc# " + docID);
          return docID;
        }

        @Override
        public boolean advanceExact(int dest) throws IOException {
          boolean exists = target.advanceExact(dest);
          out.println("\nadvanceExact(" + dest + ") -> exists# " + exists);
          return exists;
        }

        @Override
        public long cost() {
          return target.cost();
        }

        @Override
        public long nextOrd() throws IOException {
          return target.nextOrd();
        }

        @Override
        public int docValueCount() {
          return target.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
          final BytesRef val = target.lookupOrd(ord);
          out.println(val.toString() + ", ");
          return val;
        }

        @Override
        public long getValueCount() {
          return target.getValueCount();
        }
      };
    };
  }

  static GenericTermsCollector createCollectorSV(
      Function<SortedDocValues> svFunction, ScoreMode mode) {

    switch (mode) {
      case None:
        return wrap(new TermsCollector.SV(svFunction));
      case Avg:
        return new SV.Avg(svFunction);
      case Max:
      case Min:
      case Total:
      default:
        return new SV(svFunction, mode);
    }
  }

  static GenericTermsCollector wrap(final TermsCollector<?> collector) {
    return new GenericTermsCollector() {

      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return collector.getLeafCollector(context);
      }

      @Override
      public org.apache.lucene.search.ScoreMode scoreMode() {
        return collector.scoreMode();
      }

      @Override
      public BytesRefHash getCollectedTerms() {
        return collector.getCollectorTerms();
      }

      @Override
      public float[] getScoresPerTerm() {
        return null;
      }
    };
  }

  record TermsAndScores(BytesRefHash terms, float[] scores) {}

  record Manager(String fromField, boolean multipleValuesPerDocument, ScoreMode scoreMode)
      implements CollectorManager<GenericTermsCollector, TermsAndScores> {

    @Override
    public GenericTermsCollector newCollector() {
      return newGenericTermsCollector(fromField, multipleValuesPerDocument, scoreMode);
    }

    private static GenericTermsCollector newGenericTermsCollector(
        String fromField, boolean multipleValuesPerDocument, ScoreMode scoreMode) {
      final GenericTermsCollector termsWithScoreCollector;
      if (multipleValuesPerDocument) {
        Function<SortedSetDocValues> mvFunction =
            DocValuesTermsCollector.sortedSetDocValues(fromField);
        termsWithScoreCollector = GenericTermsCollector.createCollectorMV(mvFunction, scoreMode);
      } else {
        Function<SortedDocValues> svFunction = DocValuesTermsCollector.sortedDocValues(fromField);
        termsWithScoreCollector = GenericTermsCollector.createCollectorSV(svFunction, scoreMode);
      }
      return termsWithScoreCollector;
    }

    @Override
    public TermsAndScores reduce(Collection<GenericTermsCollector> collectors) {
      if (collectors.isEmpty()) {
        return new TermsAndScores(new BytesRefHash(), new float[0]);
      }
      GenericTermsCollector first = collectors.iterator().next();
      if (collectors.size() == 1) {
        return new TermsAndScores(first.getCollectedTerms(), first.getScoresPerTerm());
      }
      if (first.getScoresPerTerm() == null) {
        return reduceWithoutScores(collectors);
      } else {
        return reduceWithScores(collectors);
      }
    }

    private TermsAndScores reduceWithoutScores(Collection<GenericTermsCollector> collectors) {
      BytesRef term = new BytesRef();
      BytesRefHash terms = null;
      for (GenericTermsCollector collector : collectors) {
        if (terms == null) {
          terms = collector.getCollectedTerms();
        } else {
          BytesRefHash collectorTerms = collector.getCollectedTerms();
          for (int i = 0; i < collectorTerms.size(); i++) {
            collectorTerms.get(i, term);
            terms.add(term);
          }
        }
      }
      return new TermsAndScores(terms, null);
    }

    private TermsAndScores reduceWithScores(Collection<GenericTermsCollector> collectors) {
      BytesRef term = new BytesRef();
      BytesRefHash terms = null;
      List<int[]> termIdMap = new ArrayList<>();
      for (GenericTermsCollector collector : collectors) {
        if (terms == null) {
          terms = collector.getCollectedTerms();
          termIdMap.add(null); // represent the identity map as null
        } else {
          BytesRefHash collectorTerms = collector.getCollectedTerms();
          int[] idMap = new int[collectorTerms.size()];
          termIdMap.add(idMap);
          for (int i = 0; i < collectorTerms.size(); i++) {
            collectorTerms.get(i, term);
            int termId = terms.add(term);
            if (termId > 0) {
              idMap[i] = termId;
            } else {
              idMap[i] = -1 - termId;
            }
          }
        }
      }
      float[] scores = new float[terms.size()];
      int[] counts;
      if (scoreMode == ScoreMode.Avg) {
        counts = new int[terms.size()];
      } else {
        counts = null;
      }
      int i = 0;
      for (GenericTermsCollector collector : collectors) {
        mergeScores(collector, scores, counts, termIdMap.get(i++));
      }
      if (scoreMode == ScoreMode.Avg) {
        for (int j = 0; j < scores.length; j++) {
          scores[j] /= counts[j];
        }
      }
      return new TermsAndScores(terms, scores);
    }

    void mergeScores(
        GenericTermsCollector collector, float[] allScores, int[] allCounts, int[] idMap) {
      float[] collectorScores = collector.getScoresPerTerm();
      if (idMap == null) {
        System.arraycopy(collectorScores, 0, allScores, 0, collectorScores.length);
        if (scoreMode == ScoreMode.Avg) {
          System.arraycopy(collector.getTermCounts(), 0, allCounts, 0, collectorScores.length);
        }
        return;
      }
      switch (scoreMode) {
        case Avg ->
            mergeScoresAvg(collectorScores, allScores, collector.getTermCounts(), allCounts, idMap);
        case Total -> mergeScoresSum(collectorScores, allScores, idMap);
        case Min -> mergeScoresMin(collectorScores, allScores, idMap);
        case Max -> mergeScoresMax(collectorScores, allScores, idMap);
        case None -> throw new UnsupportedOperationException("unsupported score mode " + scoreMode);
      }
    }

    void mergeScoresMax(float[] collectorScores, float[] allScores, int[] idMap) {
      for (int j = 0; j < idMap.length; j++) {
        allScores[idMap[j]] = Math.max(collectorScores[j], allScores[idMap[j]]);
      }
    }

    void mergeScoresMin(float[] collectorScores, float[] allScores, int[] idMap) {
      for (int j = 0; j < idMap.length; j++) {
        allScores[idMap[j]] = Math.min(collectorScores[j], allScores[idMap[j]]);
      }
    }

    void mergeScoresSum(float[] collectorScores, float[] allScores, int[] idMap) {
      for (int j = 0; j < idMap.length; j++) {
        allScores[idMap[j]] += collectorScores[j];
      }
    }

    void mergeScoresAvg(
        float[] collectorScores,
        float[] allScores,
        int[] collectorCounts,
        int[] allCounts,
        int[] idMap) {
      for (int j = 0; j < idMap.length; j++) {
        allScores[idMap[j]] += collectorScores[j];
        allCounts[idMap[j]] += collectorCounts[j];
      }
    }
  }
}
