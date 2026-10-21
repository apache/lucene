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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;

abstract class GlobalOrdinalsWithScoreCollector implements Collector {

  final String field;
  final boolean doMinMax;
  final int min;
  final int max;
  final OrdinalMap ordinalMap;
  final LongBitSet collectedOrds;

  protected final Scores scores;
  protected final Occurrences occurrences;

  GlobalOrdinalsWithScoreCollector(
      String field, OrdinalMap ordinalMap, long valueCount, ScoreMode scoreMode, int min, int max) {
    if (valueCount > Integer.MAX_VALUE) {
      // We simply don't support more than
      throw new IllegalStateException("Can't collect more than [" + Integer.MAX_VALUE + "] ids");
    }
    this.field = field;
    this.doMinMax = min > 1 || max < Integer.MAX_VALUE;
    this.min = min;
    this.max = max;
    this.ordinalMap = ordinalMap;
    this.collectedOrds = new LongBitSet(valueCount);
    if (scoreMode != ScoreMode.None) {
      this.scores = new Scores(valueCount, unset());
    } else {
      this.scores = null;
    }
    if (scoreMode == ScoreMode.Avg || doMinMax) {
      this.occurrences = new Occurrences(valueCount);
    } else {
      this.occurrences = null;
    }
  }

  public boolean match(int globalOrd) {
    if (collectedOrds.get(globalOrd)) {
      if (doMinMax) {
        final int occurrence = occurrences.getOccurrence(globalOrd);
        return occurrence >= min && occurrence <= max;
      } else {
        return true;
      }
    }
    return false;
  }

  public float score(int globalOrdinal) {
    return (float) scores.getScore(globalOrdinal);
  }

  protected abstract void doScore(int globalOrd, double existingScore, double newScore);

  protected abstract double unset();

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
    if (ordinalMap != null) {
      LongValues segmentOrdToGlobalOrdLookup = ordinalMap.getGlobalOrds(context.ord);
      return new OrdinalMapCollector(docTermOrds, segmentOrdToGlobalOrdLookup);
    } else {
      return new SegmentOrdinalCollector(docTermOrds);
    }
  }

  @Override
  public org.apache.lucene.search.ScoreMode scoreMode() {
    return org.apache.lucene.search.ScoreMode.COMPLETE;
  }

  final class OrdinalMapCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;
    private final LongValues segmentOrdToGlobalOrdLookup;
    private Scorable scorer;

    OrdinalMapCollector(SortedDocValues docTermOrds, LongValues segmentOrdToGlobalOrdLookup) {
      this.docTermOrds = docTermOrds;
      this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (docTermOrds.advanceExact(doc)) {
        final int globalOrd = (int) segmentOrdToGlobalOrdLookup.get(docTermOrds.ordValue());
        collectedOrds.set(globalOrd);
        double existingScore = scores.getScore(globalOrd);
        double newScore = scorer.score();
        doScore(globalOrd, existingScore, newScore);
        if (occurrences != null) {
          occurrences.increment(globalOrd);
        }
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  final class SegmentOrdinalCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;
    private Scorable scorer;

    SegmentOrdinalCollector(SortedDocValues docTermOrds) {
      this.docTermOrds = docTermOrds;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (docTermOrds.advanceExact(doc)) {
        int segmentOrd = docTermOrds.ordValue();
        collectedOrds.set(segmentOrd);
        double existingScore = scores.getScore(segmentOrd);
        double newScore = scorer.score();
        doScore(segmentOrd, existingScore, newScore);
        if (occurrences != null) {
          occurrences.increment(segmentOrd);
        }
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  static final class Min extends GlobalOrdinalsWithScoreCollector {

    public Min(String field, OrdinalMap ordinalMap, long valueCount, int min, int max) {
      super(field, ordinalMap, valueCount, ScoreMode.Min, min, max);
    }

    @Override
    protected void doScore(int globalOrd, double existingScore, double newScore) {
      scores.setScore(globalOrd, Math.min(existingScore, newScore));
    }

    @Override
    protected double unset() {
      return Double.POSITIVE_INFINITY;
    }
  }

  static final class Max extends GlobalOrdinalsWithScoreCollector {

    public Max(String field, OrdinalMap ordinalMap, long valueCount, int min, int max) {
      super(field, ordinalMap, valueCount, ScoreMode.Max, min, max);
    }

    @Override
    protected void doScore(int globalOrd, double existingScore, double newScore) {
      scores.setScore(globalOrd, Math.max(existingScore, newScore));
    }

    @Override
    protected double unset() {
      return Double.NEGATIVE_INFINITY;
    }
  }

  static final class Sum extends GlobalOrdinalsWithScoreCollector {

    public Sum(String field, OrdinalMap ordinalMap, long valueCount, int min, int max) {
      super(field, ordinalMap, valueCount, ScoreMode.Total, min, max);
    }

    @Override
    protected void doScore(int globalOrd, double existingScore, double newScore) {
      scores.setScore(globalOrd, existingScore + newScore);
    }

    @Override
    protected double unset() {
      return 0d;
    }
  }

  static final class Avg extends GlobalOrdinalsWithScoreCollector {

    public Avg(String field, OrdinalMap ordinalMap, long valueCount, int min, int max) {
      super(field, ordinalMap, valueCount, ScoreMode.Avg, min, max);
    }

    @Override
    protected void doScore(int globalOrd, double existingScore, double newScore) {
      scores.setScore(globalOrd, existingScore + newScore);
    }

    @Override
    public float score(int globalOrdinal) {
      return (float) (scores.getScore(globalOrdinal) / occurrences.getOccurrence(globalOrdinal));
    }

    @Override
    protected double unset() {
      return 0d;
    }
  }

  static final class NoScore extends GlobalOrdinalsWithScoreCollector {

    public NoScore(String field, OrdinalMap ordinalMap, long valueCount, int min, int max) {
      super(field, ordinalMap, valueCount, ScoreMode.None, min, max);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
      if (ordinalMap != null) {
        LongValues segmentOrdToGlobalOrdLookup = ordinalMap.getGlobalOrds(context.ord);
        return new LeafCollector() {

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            if (docTermOrds.advanceExact(doc)) {
              final int globalOrd = (int) segmentOrdToGlobalOrdLookup.get(docTermOrds.ordValue());
              collectedOrds.set(globalOrd);
              occurrences.increment(globalOrd);
            }
          }
        };
      } else {
        return new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            if (docTermOrds.advanceExact(doc)) {
              int segmentOrd = docTermOrds.ordValue();
              collectedOrds.set(segmentOrd);
              occurrences.increment(segmentOrd);
            }
          }
        };
      }
    }

    @Override
    protected void doScore(int globalOrd, double existingScore, double newScore) {}

    @Override
    public float score(int globalOrdinal) {
      return 1f;
    }

    @Override
    protected double unset() {
      return 0d;
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
      return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  // Because the global ordinal is directly used as a key to a score we should be somewhat smart
  // about allocation
  // the scores array. Most of the times not all docs match so splitting the scores array up in
  // blocks can prevent creation of huge arrays.
  // Also working with smaller arrays is supposed to be more gc friendly
  //
  // At first a hash map implementation would make sense, but in the case that more than half of
  // docs match this becomes more expensive
  // then just using an array.

  // Maybe this should become a method parameter?
  static final int arraySize = 4096;

  static final class Scores {

    // Accumulated in double precision because float addition isn't associative, so summing the
    // same per-document scores in a different order may round to a different float.
    final double[][] blocks;
    final double unset;

    private Scores(long valueCount, double unset) {
      long blockSize = valueCount + arraySize - 1;
      blocks = new double[(int) ((blockSize) / arraySize)][];
      this.unset = unset;
    }

    public void setScore(int globalOrdinal, double score) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      double[] scores = blocks[block];
      if (scores == null) {
        blocks[block] = scores = new double[arraySize];
        if (unset != 0d) {
          Arrays.fill(scores, unset);
        }
      }
      scores[offset] = score;
    }

    public double getScore(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      double[] scores = blocks[block];
      double score;
      if (scores != null) {
        score = scores[offset];
      } else {
        score = unset;
      }
      return score;
    }
  }

  static final class Occurrences {

    final int[][] blocks;

    private Occurrences(long valueCount) {
      long blockSize = valueCount + arraySize - 1;
      blocks = new int[(int) (blockSize / arraySize)][];
    }

    public void increment(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      int[] occurrences = blocks[block];
      if (occurrences == null) {
        blocks[block] = occurrences = new int[arraySize];
      }
      occurrences[offset]++;
    }

    public int getOccurrence(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      int[] occurrences = blocks[block];
      return occurrences[offset];
    }
  }
}
