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
import java.util.function.LongToDoubleFunction;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * Provides some concrete implementations of {@link DoubleValuesSource} class to be used by
 * different APIs
 */
final class DoubleValuesSourceUtil {

  static final DoubleValuesSource SCORES =
      new DoubleValuesSource() {
        @Override
        public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
            throws IOException {
          assert scores != null;
          return scores;
        }

        @Override
        public boolean needsScores() {
          return true;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }

        @Override
        public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) {
          return scoreExplanation;
        }

        @Override
        public int hashCode() {
          return 0;
        }

        @Override
        public boolean equals(Object obj) {
          return obj == this;
        }

        @Override
        public String toString() {
          return "scores";
        }

        @Override
        public DoubleValuesSource rewrite(IndexSearcher searcher) {
          return this;
        }
      };

  static class ConstantValuesSource extends DoubleValuesSource {

    private final DoubleValues doubleValues;
    private final double value;

    ConstantValuesSource(double value) {
      this.value = value;
      this.doubleValues =
          new DoubleValues() {
            @Override
            public double doubleValue() {
              return value;
            }

            @Override
            public boolean advanceExact(int doc) {
              return true;
            }
          };
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) {
      return this;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return doubleValues;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) {
      return Explanation.match(value, "constant(" + value + ")");
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConstantValuesSource that = (ConstantValuesSource) o;
      return Double.compare(that.value, value) == 0;
    }

    @Override
    public String toString() {
      return "constant(" + value + ")";
    }
  }

  static class FieldValuesSource extends DoubleValuesSource {

    final String field;
    final LongToDoubleFunction decoder;

    FieldValuesSource(String field, LongToDoubleFunction decoder) {
      this.field = field;
      this.decoder = decoder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldValuesSource that = (FieldValuesSource) o;
      return Objects.equals(field, that.field) && Objects.equals(decoder, that.decoder);
    }

    @Override
    public String toString() {
      return "double(" + field + ")";
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      final NumericDocValues values = DocValues.getNumeric(ctx.reader(), field);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return decoder.applyAsDouble(values.longValue());
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return values.advanceExact(target);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match(values.doubleValue(), this.toString());
      else return Explanation.noMatch(this.toString());
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }
  }

  static class QueryDoubleValuesSource extends DoubleValuesSource {

    private final Query query;

    QueryDoubleValuesSource(Query query) {
      this.query = query;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueryDoubleValuesSource that = (QueryDoubleValuesSource) o;
      return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
      return Objects.hash(query);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      throw new UnsupportedOperationException("This DoubleValuesSource must be rewritten");
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new WeightDoubleValuesSource(
          searcher.rewrite(query).createWeight(searcher, ScoreMode.COMPLETE, 1f));
    }

    @Override
    public String toString() {
      return "score(" + query.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  static class WeightDoubleValuesSource extends DoubleValuesSource {

    private final Weight weight;

    private WeightDoubleValuesSource(Weight weight) {
      this.weight = weight;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      Scorer scorer = weight.scorer(ctx);
      if (scorer == null) return DoubleValues.EMPTY;

      return new DoubleValues() {
        private final TwoPhaseIterator tpi = scorer.twoPhaseIterator();
        private final DocIdSetIterator disi =
            (tpi == null) ? scorer.iterator() : tpi.approximation();
        private Boolean tpiMatch = null; // cache tpi.matches()

        @Override
        public double doubleValue() throws IOException {
          return scorer.score();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          if (disi.docID() < doc) {
            disi.advance(doc);
            tpiMatch = null;
          }
          if (disi.docID() == doc) {
            if (tpi == null) {
              return true;
            } else if (tpiMatch == null) {
              tpiMatch = tpi.matches();
            }
            return tpiMatch;
          }
          return false;
        }
      };
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      return weight.explain(ctx, docId);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WeightDoubleValuesSource that = (WeightDoubleValuesSource) o;
      return Objects.equals(weight, that.weight);
    }

    @Override
    public int hashCode() {
      return Objects.hash(weight);
    }

    @Override
    public String toString() {
      return "score(" + weight.parentQuery.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }
}
