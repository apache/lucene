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
import java.util.function.DoubleToLongFunction;
import java.util.function.LongToDoubleFunction;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.comparators.DoubleComparator;

/**
 * Base class for producing {@link DoubleValues}
 *
 * <p>To obtain a {@link DoubleValues} object for a leaf reader, clients should call {@link
 * #rewrite(IndexSearcher)} against the top-level searcher, and then call {@link
 * #getValues(LeafReaderContext, DoubleValues)} on the resulting DoubleValuesSource.
 *
 * <p>DoubleValuesSource objects for NumericDocValues fields can be obtained by calling {@link
 * #fromDoubleField(String)}, {@link #fromFloatField(String)}, {@link #fromIntField(String)} or
 * {@link #fromLongField(String)}, or from {@link #fromField(String, LongToDoubleFunction)} if
 * special long-to-double encoding is required.
 *
 * <p>Scores may be used as a source for value calculations by wrapping a {@link Scorer} using
 * {@link #fromScorer(Scorable)} and passing the resulting DoubleValues to {@link
 * #getValues(LeafReaderContext, DoubleValues)}. The scores can then be accessed using the {@link
 * #SCORES} DoubleValuesSource.
 *
 * <p>Also see {@link DoubleValuesSourceUtil} class that provides some concrete implementations of
 * this class used by APIs defined in this class.
 */
public abstract class DoubleValuesSource implements SegmentCacheable {

  /**
   * Returns a {@link DoubleValues} instance for the passed-in LeafReaderContext and scores
   *
   * <p>If scores are not needed to calculate the values (ie {@link #needsScores() returns false},
   * callers may safely pass {@code null} for the {@code scores} parameter.
   */
  public abstract DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
      throws IOException;

  /** Return true if document scores are needed to calculate values */
  public abstract boolean needsScores();

  /**
   * An explanation of the value for the named document.
   *
   * @param ctx the readers context to create the {@link Explanation} for.
   * @param docId the document's id relative to the given context's reader
   * @return an Explanation for the value
   * @throws IOException if an {@link IOException} occurs
   */
  public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
      throws IOException {
    DoubleValues dv =
        getValues(
            ctx,
            DoubleValuesSource.constant(scoreExplanation.getValue().doubleValue())
                .getValues(ctx, null));
    if (dv.advanceExact(docId)) return Explanation.match(dv.doubleValue(), this.toString());
    return Explanation.noMatch(this.toString());
  }

  /**
   * Return a DoubleValuesSource specialised for the given IndexSearcher
   *
   * <p>Implementations should assume that this will only be called once. IndexReader-independent
   * implementations can just return {@code this}
   *
   * <p>Queries that use DoubleValuesSource objects should call rewrite() during {@link
   * Query#createWeight(IndexSearcher, ScoreMode, float)} rather than during {@link
   * Query#rewrite(IndexSearcher)} to avoid IndexReader reference leakage.
   *
   * <p>For the same reason, implementations that cache references to the IndexSearcher should
   * return a new object from this method.
   */
  public abstract DoubleValuesSource rewrite(IndexSearcher reader) throws IOException;

  /**
   * Create a sort field based on the value of this producer
   *
   * @param reverse true if the sort should be decreasing
   */
  public SortField getSortField(boolean reverse) {
    return new DoubleValuesSortField(this, reverse);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /** Convert to a LongValuesSource by casting the double values to longs */
  public final LongValuesSource toLongValuesSource() {
    return new LongValuesSource.LongDoubleValuesSource(this);
  }

  /**
   * Creates a DoubleValuesSource that wraps a generic NumericDocValues field
   *
   * @param field the field to wrap, must have NumericDocValues
   * @param decoder a function to convert the long-valued doc values to doubles
   */
  public static DoubleValuesSource fromField(String field, LongToDoubleFunction decoder) {
    return new DoubleValuesSourceUtil.FieldValuesSource(field, decoder);
  }

  /** Creates a DoubleValuesSource that wraps a double-valued field */
  public static DoubleValuesSource fromDoubleField(String field) {
    return fromField(field, Double::longBitsToDouble);
  }

  /** Creates a DoubleValuesSource that wraps a float-valued field */
  public static DoubleValuesSource fromFloatField(String field) {
    return fromField(field, (v) -> (double) Float.intBitsToFloat((int) v));
  }

  /** Creates a DoubleValuesSource that wraps a long-valued field */
  public static DoubleValuesSource fromLongField(String field) {
    return fromField(field, (v) -> (double) v);
  }

  /** Creates a DoubleValuesSource that wraps an int-valued field */
  public static DoubleValuesSource fromIntField(String field) {
    return fromLongField(field);
  }

  /**
   * A DoubleValuesSource that exposes a document's score
   *
   * <p>If this source is used as part of a values calculation, then callers must not pass {@code
   * null} as the {@link DoubleValues} parameter on {@link #getValues(LeafReaderContext,
   * DoubleValues)}
   */
  public static final DoubleValuesSource SCORES = DoubleValuesSourceUtil.SCORES;

  /** Creates a DoubleValuesSource that always returns a constant value */
  public static DoubleValuesSource constant(double value) {
    return new DoubleValuesSourceUtil.ConstantValuesSource(value);
  }

  /**
   * Returns a DoubleValues instance that wraps scores returned by a Scorer.
   *
   * <p>Note: If you intend to call {@link Scorable#score()} on the provided {@code scorer}
   * separately, you may want to consider wrapping the collector with {@link
   * ScoreCachingWrappingScorer#wrap(LeafCollector)} to avoid computing the actual score multiple
   * times.
   */
  public static DoubleValues fromScorer(Scorable scorer) {
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return scorer.score();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return true;
      }
    };
  }

  private static class DoubleValuesSortField extends SortField {

    final DoubleValuesSource producer;

    DoubleValuesSortField(DoubleValuesSource producer, boolean reverse) {
      super(producer.toString(), new DoubleValuesComparatorSource(producer), reverse);
      this.producer = producer;
    }

    @Override
    public void setMissingValue(Object missingValue) {
      if (missingValue instanceof Number) {
        this.missingValue = missingValue;
        ((DoubleValuesComparatorSource) getComparatorSource())
            .setMissingValue(((Number) missingValue).doubleValue());
      } else {
        super.setMissingValue(missingValue);
      }
    }

    @Override
    public boolean needsScores() {
      return producer.needsScores();
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder("<");
      buffer.append(getField()).append(">");
      if (reverse) buffer.append("!");
      return buffer.toString();
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      DoubleValuesSource rewrittenSource = producer.rewrite(searcher);
      if (rewrittenSource == producer) {
        return this;
      }
      DoubleValuesSortField rewritten = new DoubleValuesSortField(rewrittenSource, reverse);
      if (missingValue != null) {
        rewritten.setMissingValue(missingValue);
      }
      return rewritten;
    }
  }

  private static class DoubleValuesHolder {
    DoubleValues values;
  }

  private static class DoubleValuesComparatorSource extends FieldComparatorSource {
    private final DoubleValuesSource producer;
    private double missingValue;

    DoubleValuesComparatorSource(DoubleValuesSource producer) {
      this.producer = producer;
      this.missingValue = 0d;
    }

    void setMissingValue(double missingValue) {
      this.missingValue = missingValue;
    }

    @Override
    public FieldComparator<Double> newComparator(
        String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
      return new DoubleComparator(numHits, fieldname, missingValue, reversed, false) {
        @Override
        public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
          DoubleValuesHolder holder = new DoubleValuesHolder();

          return new DoubleComparator.DoubleLeafComparator(context) {
            LeafReaderContext ctx;

            @Override
            protected NumericDocValues getNumericDocValues(
                LeafReaderContext context, String field) {
              ctx = context;
              return asNumericDocValues(holder, Double::doubleToLongBits);
            }

            @Override
            public void setScorer(Scorable scorer) throws IOException {
              holder.values = producer.getValues(ctx, fromScorer(scorer));
              super.setScorer(scorer);
            }
          };
        }
      };
    }
  }

  private static NumericDocValues asNumericDocValues(
      DoubleValuesHolder in, DoubleToLongFunction converter) {
    return new NumericDocValues() {
      @Override
      public long longValue() throws IOException {
        return converter.applyAsLong(in.values.doubleValue());
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return in.values.advanceExact(target);
      }

      @Override
      public int docID() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Create a DoubleValuesSource that returns the score of a particular query */
  public static DoubleValuesSource fromQuery(Query query) {
    return new DoubleValuesSourceUtil.QueryDoubleValuesSource(query);
  }
}
