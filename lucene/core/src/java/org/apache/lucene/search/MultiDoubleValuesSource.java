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
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * Base class for producing {@link MultiDoubleValues}. See also {@link DoubleValuesSource} for a
 * single-valued version.
 *
 * <p>To obtain a {@link MultiDoubleValues} object for a leaf reader, clients should call {@link
 * #rewrite(IndexSearcher)} against the top-level searcher, and then {@link
 * #getValues(LeafReaderContext, DoubleValues)}.
 *
 * <p>MultiDoubleValuesSource objects for NumericDocValues/SortedNumericDocValues fields can be
 * obtained by calling {@link #fromFloatField(String)}, {@link #fromDoubleField(String)}, {@link
 * #fromIntField(String)}, or {@link #fromLongField(String)}. If custom long-to-double logic is
 * required, {@link #fromField(String, LongToDoubleFunction)} can be used. This is valid for both
 * multi-valued and single-valued fields.
 *
 * <p>Scores may be used as a source for value calculations by wrapping a {@link Scorer} using
 * {@link DoubleValuesSource#fromScorer(Scorable)} and passing the resulting DoubleValues to {@link
 * #getValues(LeafReaderContext, DoubleValues)}.
 *
 * <p>To obtain a MultiDoubleValuesSource from an existing {@link DoubleValuesSource}, see {@link
 * #fromSingleValued(DoubleValuesSource)}. Instances created in this way can be "unwrapped" using
 * {@link #unwrapSingleton(MultiDoubleValuesSource)} if necessary.
 */
// TODO: Add support for converting to single-valued (e.g., by min/max/sum/avg/etc)?
public abstract class MultiDoubleValuesSource implements SegmentCacheable {

  /**
   * Returns a {@link MultiDoubleValues} instance for the passed-in LeafReaderContext and scores
   *
   * <p>If scores are not needed to calculate the values (ie {@link #needsScores() returns false},
   * callers may safely pass {@code null} for the {@code scores} parameter.
   */
  public abstract MultiDoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
      throws IOException;

  /** Return true if document scores are needed to calculate values */
  public abstract boolean needsScores();

  /**
   * Return a MultiDoubleValuesSource specialised for the given IndexSearcher
   *
   * <p>Implementations should assume that this will only be called once. IndexSearcher-independent
   * implementations can just return {@code this}
   */
  public abstract MultiDoubleValuesSource rewrite(IndexSearcher searcher) throws IOException;

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Creates a MultiDoubleValuesSource that wraps a generic NumericDocValues/SortedNumericDocValues
   * field. Uses the long-to-double decoding logic specified in {@code decoder} for converting the
   * stored value to a double.
   */
  public static MultiDoubleValuesSource fromField(String field, LongToDoubleFunction decoder) {
    return new FieldMultiValuedSource(field, decoder);
  }

  /** Creates a MultiDoubleValuesSource that wraps a double-valued field */
  public static MultiDoubleValuesSource fromDoubleField(String field) {
    return fromField(field, Double::longBitsToDouble);
  }

  /** Creates a MultiDoubleValuesSource that wraps a float-valued field */
  public static MultiDoubleValuesSource fromFloatField(String field) {
    return fromField(field, v -> (double) Float.intBitsToFloat((int) v));
  }

  /** Creates a MultiDoubleValuesSource that wraps a long-valued field */
  public static MultiDoubleValuesSource fromLongField(String field) {
    return fromField(field, v -> (double) v);
  }

  /** Creates a MultiDoubleValuesSource that wraps an int-valued field */
  public static MultiDoubleValuesSource fromIntField(String field) {
    return fromLongField(field);
  }

  /** Creates a MultiDoubleValuesSource that wraps a single-valued {@code DoubleValuesSource} */
  public static MultiDoubleValuesSource fromSingleValued(DoubleValuesSource singleValued) {
    return new SingleValuedAsMultiValued(singleValued);
  }

  /**
   * Returns a single-valued view of the {@code MultiDoubleValuesSource} if it was previously
   * wrapped with {@link #fromSingleValued(DoubleValuesSource)}, or null.
   */
  public static DoubleValuesSource unwrapSingleton(MultiDoubleValuesSource in) {
    if (in instanceof SingleValuedAsMultiValued) {
      return ((SingleValuedAsMultiValued) in).in;
    } else {
      return null;
    }
  }

  /** Convert to a MultiLongValuesSource by casting the double values to longs */
  public final MultiLongValuesSource toMultiLongValuesSource() {
    return new LongDoubleValuesSource(this);
  }

  private static class FieldMultiValuedSource extends MultiDoubleValuesSource {
    private final String field;
    private final LongToDoubleFunction decoder;

    FieldMultiValuedSource(String field, LongToDoubleFunction decoder) {
      this.field = field;
      this.decoder = decoder;
    }

    @Override
    public MultiDoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), field);
      return new MultiDoubleValues() {
        @Override
        public long getValueCount() {
          return docValues.docValueCount();
        }

        @Override
        public double nextValue() throws IOException {
          return decoder.applyAsDouble(docValues.nextValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return docValues.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public MultiDoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldMultiValuedSource that = (FieldMultiValuedSource) o;
      return Objects.equals(field, that.field) && Objects.equals(decoder, that.decoder);
    }

    @Override
    public String toString() {
      return "multi-double(" + field + ")";
    }
  }

  private static class SingleValuedAsMultiValued extends MultiDoubleValuesSource {
    private final DoubleValuesSource in;

    SingleValuedAsMultiValued(DoubleValuesSource in) {
      this.in = in;
    }

    @Override
    public MultiDoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final DoubleValues singleValues = in.getValues(ctx, scores);
      return new MultiDoubleValues() {
        @Override
        public long getValueCount() {
          return 1;
        }

        @Override
        public double nextValue() throws IOException {
          return singleValues.doubleValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return singleValues.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return in.needsScores();
    }

    @Override
    public MultiDoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      DoubleValuesSource rewritten = in.rewrite(searcher);
      if (rewritten != in) {
        return new SingleValuedAsMultiValued(rewritten);
      } else {
        return this;
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return in.isCacheable(ctx);
    }

    @Override
    public int hashCode() {
      return Objects.hash(in);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SingleValuedAsMultiValued that = (SingleValuedAsMultiValued) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public String toString() {
      return "multi-double(" + in + ")";
    }
  }

  private static class LongDoubleValuesSource extends MultiLongValuesSource {
    private final MultiDoubleValuesSource in;

    LongDoubleValuesSource(MultiDoubleValuesSource in) {
      this.in = in;
    }

    @Override
    public MultiLongValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final MultiDoubleValues doubleValues = in.getValues(ctx, scores);
      return new MultiLongValues() {
        @Override
        public long getValueCount() {
          return doubleValues.getValueCount();
        }

        @Override
        public long nextValue() throws IOException {
          return (long) doubleValues.nextValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return doubleValues.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return in.needsScores();
    }

    @Override
    public MultiLongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      MultiDoubleValuesSource rewritten = in.rewrite(searcher);
      if (rewritten != in) {
        return new LongDoubleValuesSource(rewritten);
      } else {
        return this;
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return in.isCacheable(ctx);
    }

    @Override
    public int hashCode() {
      return Objects.hash(in);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LongDoubleValuesSource that = (LongDoubleValuesSource) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public String toString() {
      return "multi-double(" + in + ")";
    }
  }
}
