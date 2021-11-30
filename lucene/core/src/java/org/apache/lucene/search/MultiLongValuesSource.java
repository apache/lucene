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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * Base class for producing {@link MultiLongValues}. See also {@link LongValuesSource} for a
 * single-valued version.
 *
 * <p>To obtain a {@link MultiLongValues} object for a leaf reader, clients should call {@link
 * #rewrite(IndexSearcher)} against the top-level searcher, and then {@link
 * #getValues(LeafReaderContext, DoubleValues)}.
 *
 * <p>MultiLongValuesSource objects for long and int-valued NumericDocValues/SortedNumericDocValues
 * fields can be obtained by calling {@link #fromLongField(String)} and {@link
 * #fromIntField(String)}. This is valid for both multi-valued and single-valued fields.
 *
 * <p>To obtain a MultiLongValuesSource from a float or double-valued
 * NumericDocValues/SortedNumericDocValues field, use {@link
 * MultiDoubleValuesSource#fromFloatField(String)} or {@link
 * MultiDoubleValuesSource#fromDoubleField(String)} and then call {@link
 * MultiDoubleValuesSource#toMultiLongValuesSource()}.
 *
 * <p>To obtain a MultiLongValuesSource from an existing {@link LongValuesSource}, see {@link
 * #fromSingleValued(LongValuesSource)}. Instances created in this way can be "unwrapped" using
 * {@link #unwrapSingleton(MultiLongValuesSource)} if necessary.
 */
// TODO: Add support for converting to single-valued (e.g., by min/max/sum/avg/etc)?
public abstract class MultiLongValuesSource implements SegmentCacheable {

  /**
   * Returns a {@link MultiLongValues} instance for the passed-in LeafReaderContext and scores
   *
   * <p>If scores are not needed to calculate the values (ie {@link #needsScores() returns false},
   * callers may safely pass {@code null} for the {@code scores} parameter.
   */
  public abstract MultiLongValues getValues(LeafReaderContext ctx, DoubleValues scores)
      throws IOException;

  /** Return true if document scores are needed to calculate values */
  public abstract boolean needsScores();

  /**
   * Return a MultiLongValuesSource specialised for the given IndexSearcher
   *
   * <p>Implementations should assume that this will only be called once. IndexSearcher-independent
   * implementations can just return {@code this}
   */
  public abstract MultiLongValuesSource rewrite(IndexSearcher searcher) throws IOException;

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /** Creates a MultiLongValuesSource that wraps a long-valued field */
  public static MultiLongValuesSource fromLongField(String field) {
    return new FieldMultiValueSource(field);
  }

  /** Creates a MultiLongValuesSource that wraps an int-valued field */
  public static MultiLongValuesSource fromIntField(String field) {
    return fromLongField(field);
  }

  /** Creates a MultiLongValuesSource that wraps a single-valued {@code LongValuesSource} */
  public static MultiLongValuesSource fromSingleValued(LongValuesSource singleValued) {
    return new SingleValuedAsMultiValued(singleValued);
  }

  /**
   * Returns a single-valued view of the {@code MultiLongValuesSource} if it was previously wrapped
   * with {@link #fromSingleValued(LongValuesSource)}, or null.
   */
  public static LongValuesSource unwrapSingleton(MultiLongValuesSource in) {
    if (in instanceof SingleValuedAsMultiValued) {
      return ((SingleValuedAsMultiValued) in).in;
    } else {
      return null;
    }
  }

  /** Convert to a MultiDoubleValuesSource by casting long values to doubles */
  public final MultiDoubleValuesSource toMultiDoubleValuesSource() {
    return new DoubleLongValuesSources(this);
  }

  private static class FieldMultiValueSource extends MultiLongValuesSource {
    private final String field;

    FieldMultiValueSource(String field) {
      this.field = field;
    }

    @Override
    public MultiLongValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), field);
      return new MultiLongValues() {
        @Override
        public long getValueCount() {
          return docValues.docValueCount();
        }

        @Override
        public long nextValue() throws IOException {
          return docValues.nextValue();
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
    public MultiLongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldMultiValueSource that = (FieldMultiValueSource) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public String toString() {
      return "multi-long(" + field + ")";
    }
  }

  private static class SingleValuedAsMultiValued extends MultiLongValuesSource {
    private final LongValuesSource in;

    SingleValuedAsMultiValued(LongValuesSource in) {
      this.in = in;
    }

    @Override
    public MultiLongValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final LongValues singleValued = in.getValues(ctx, scores);
      return new MultiLongValues() {
        @Override
        public long getValueCount() {
          return 1;
        }

        @Override
        public long nextValue() throws IOException {
          return singleValued.longValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return singleValued.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return in.needsScores();
    }

    @Override
    public MultiLongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      LongValuesSource rewritten = in.rewrite(searcher);
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
      return "multi-long(" + in + ")";
    }
  }

  private static class DoubleLongValuesSources extends MultiDoubleValuesSource {
    private final MultiLongValuesSource in;

    DoubleLongValuesSources(MultiLongValuesSource in) {
      this.in = in;
    }

    @Override
    public MultiDoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      final MultiLongValues longValues = in.getValues(ctx, scores);
      return new MultiDoubleValues() {
        @Override
        public long getValueCount() {
          return longValues.getValueCount();
        }

        @Override
        public double nextValue() throws IOException {
          return (double) longValues.nextValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return longValues.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return in.needsScores();
    }

    @Override
    public MultiDoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      MultiLongValuesSource rewritten = in.rewrite(searcher);
      if (rewritten != in) {
        return new DoubleLongValuesSources(rewritten);
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
      DoubleLongValuesSources that = (DoubleLongValuesSources) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public String toString() {
      return "multi-long(" + in + ")";
    }
  }
}
