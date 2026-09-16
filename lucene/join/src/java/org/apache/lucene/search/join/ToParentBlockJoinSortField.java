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

import static org.apache.lucene.search.join.BlockJoinSelector.toIter;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.apache.lucene.search.comparators.FloatComparator;
import org.apache.lucene.search.comparators.IntComparator;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.search.comparators.TermOrdValComparator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.NumericUtils;

/**
 * A special sort field that allows sorting parent docs based on nested / child level fields. Based
 * on the sort order it either takes the document with the lowest or highest field value into
 * account.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinSortField extends SortField {

  private final boolean reverseChildren;
  private final BitSetProducer parentFilter;
  private final BitSetProducer childFilter;
  private final Object childMissingValue;

  /**
   * Create ToParentBlockJoinSortField. The parent and child document ordering is based on the flag
   * reverse. Missing values are treated as the default for the type.
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on both child and parent levels.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(
      String field,
      Type type,
      boolean reverse,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    this(field, type, reverse, null, null, parentFilter, childFilter);
  }

  /**
   * Create ToParentBlockJoinSortField. Missing values are treated as the default for the type.
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverseParents Whether natural order should be reversed on the parent level.
   * @param reverseChildren Whether natural order should be reversed on the nested / child document
   *     level.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(
      String field,
      Type type,
      boolean reverseParents,
      boolean reverseChildren,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    this(field, type, reverseParents, reverseChildren, null, null, parentFilter, childFilter);
  }

  /**
   * Create ToParentBlockJoinSortField. The parent document ordering is based on child document
   * ordering (reverse).
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on both child and parent levels.
   * @param parentMissingValue The missing value for parent documents that have no child documents.
   *     For Type.STRING use {@link SortField#STRING_FIRST} or {@link SortField#STRING_LAST}. For
   *     numeric types use the corresponding boxed type (e.g. {@link Integer} for Type.INT). Pass
   *     {@code null} for default behavior.
   * @param childMissingValue The missing value for child documents that lack the sort field. This
   *     value participates in the min/max selection among siblings. Type constraints are the same
   *     as for {@code parentMissingValue}. Pass {@code null} to skip children without a value.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(
      String field,
      Type type,
      boolean reverse,
      Object parentMissingValue,
      Object childMissingValue,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    this(
        field,
        type,
        reverse,
        reverse,
        parentMissingValue,
        childMissingValue,
        parentFilter,
        childFilter);
  }

  /**
   * Create ToParentBlockJoinSortField.
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverseParents Whether natural order should be reversed on the parent level.
   * @param reverseChildren Whether natural order should be reversed on the nested / child document
   *     level.
   * @param parentMissingValue The missing value for parent documents whose children do not have a
   *     value for the sort field. For Type.STRING use {@link SortField#STRING_FIRST} or {@link
   *     SortField#STRING_LAST}. For numeric types use the corresponding boxed type (e.g. {@link
   *     Integer} for Type.INT). Pass {@code null} for default behavior.
   * @param childMissingValue The missing value for child documents that lack the sort field. This
   *     value participates in the min/max selection among siblings. Type constraints are the same
   *     as for {@code parentMissingValue}. Pass {@code null} to skip children without a value.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(
      String field,
      Type type,
      boolean reverseParents,
      boolean reverseChildren,
      Object parentMissingValue,
      Object childMissingValue,
      BitSetProducer parentFilter,
      BitSetProducer childFilter) {
    super(field, type, reverseParents, parentMissingValue);
    validate(type, childMissingValue);
    this.reverseChildren = reverseChildren;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
    this.childMissingValue = childMissingValue;
  }

  private static void validate(Type type, Object childMissingValue) {
    switch (type) {
      case STRING -> validateMissingValue(type, String.class, childMissingValue);
      case DOUBLE -> validateMissingValue(type, Double.class, childMissingValue);
      case FLOAT -> validateMissingValue(type, Float.class, childMissingValue);
      case LONG -> validateMissingValue(type, Long.class, childMissingValue);
      case INT -> validateMissingValue(type, Integer.class, childMissingValue);
      // $CASES-OMITTED$
      default -> throw new UnsupportedOperationException("Sort type " + type + " is not supported");
    }
  }

  private static <T> void validateMissingValue(Type type, Class<T> clazz, Object missingValue) {
    if (missingValue == null) {
      return;
    }
    if (type == Type.STRING) {
      if (missingValue != STRING_FIRST && missingValue != STRING_LAST) {
        throw new IllegalArgumentException(
            "For Type.STRING, missing value must be either STRING_FIRST or STRING_LAST");
      }
      return;
    }
    if (!(clazz.isInstance(missingValue))) {
      throw new IllegalArgumentException(
          "Missing value for "
              + type.getDeclaringClass().getName()
              + " must be a "
              + clazz.getName()
              + ", got "
              + missingValue.getClass().getName());
    }
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    return switch (getType()) {
      case STRING -> getStringComparator(numHits);
      case DOUBLE -> getDoubleComparator(numHits);
      case FLOAT -> getFloatComparator(numHits);
      case LONG -> getLongComparator(numHits);
      case INT -> getIntComparator(numHits);
      // $CASES-OMITTED$
      default ->
          throw new UnsupportedOperationException("Sort type " + getType() + " is not supported");
    };
  }

  private FieldComparator<?> getStringComparator(int numHits) {
    return new TermOrdValComparator(
        numHits, getField(), missingValue == STRING_LAST, getReverse(), Pruning.NONE) {
      @Override
      protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
          throws IOException {
        SortedSetDocValues sortedSet = DocValues.getSortedSet(context.reader(), field);
        final BlockJoinSelector.Type type =
            reverseChildren ? BlockJoinSelector.Type.MAX : BlockJoinSelector.Type.MIN;
        final BitSet parents = parentFilter.getBitSet(context);
        final BitSet children = childFilter.getBitSet(context);
        if (children == null) {
          return DocValues.emptySorted();
        }
        return BlockJoinSelector.wrap(
            sortedSet, type, parents, toIter(children), childMissingValue == STRING_LAST);
      }
    };
  }

  private FieldComparator<?> getIntComparator(int numHits) {
    return new IntComparator(
        numHits, getField(), (Integer) missingValue, getReverse(), Pruning.NONE) {
      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new IntLeafComparator(context) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
              throws IOException {
            SortedNumericDocValues sortedNumeric =
                DocValues.getSortedNumeric(context.reader(), field);
            final BlockJoinSelector.Type type =
                reverseChildren ? BlockJoinSelector.Type.MAX : BlockJoinSelector.Type.MIN;
            final BitSet parents = parentFilter.getBitSet(context);
            final BitSet children = childFilter.getBitSet(context);
            if (children == null) {
              return DocValues.emptyNumeric();
            }
            return BlockJoinSelector.wrap(
                sortedNumeric,
                type,
                parents,
                toIter(children),
                childMissingValue == null ? null : Long.valueOf((Integer) childMissingValue));
          }
        };
      }
    };
  }

  private FieldComparator<?> getLongComparator(int numHits) {
    return new LongComparator(
        numHits, getField(), (Long) missingValue, getReverse(), Pruning.NONE) {
      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new LongLeafComparator(context) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
              throws IOException {
            SortedNumericDocValues sortedNumeric =
                DocValues.getSortedNumeric(context.reader(), field);
            final BlockJoinSelector.Type type =
                reverseChildren ? BlockJoinSelector.Type.MAX : BlockJoinSelector.Type.MIN;
            final BitSet parents = parentFilter.getBitSet(context);
            final BitSet children = childFilter.getBitSet(context);
            if (children == null) {
              return DocValues.emptyNumeric();
            }
            return BlockJoinSelector.wrap(
                sortedNumeric, type, parents, toIter(children), (Long) childMissingValue);
          }
        };
      }
    };
  }

  private FieldComparator<?> getFloatComparator(int numHits) {
    return new FloatComparator(
        numHits, getField(), (Float) missingValue, getReverse(), Pruning.NONE) {
      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new FloatLeafComparator(context) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
              throws IOException {
            SortedNumericDocValues sortedNumeric =
                DocValues.getSortedNumeric(context.reader(), field);
            final BlockJoinSelector.Type type =
                reverseChildren ? BlockJoinSelector.Type.MAX : BlockJoinSelector.Type.MIN;
            final BitSet parents = parentFilter.getBitSet(context);
            final BitSet children = childFilter.getBitSet(context);
            if (children == null) {
              return DocValues.emptyNumeric();
            }
            return new FilterNumericDocValues(
                BlockJoinSelector.wrap(
                    sortedNumeric,
                    type,
                    parents,
                    toIter(children),
                    childMissingValue == null
                        ? null
                        : (long) NumericUtils.floatToSortableInt((Float) childMissingValue))) {
              @Override
              public long longValue() throws IOException {
                // undo the numericutils sortability
                return NumericUtils.sortableFloatBits((int) super.longValue());
              }
            };
          }
        };
      }
    };
  }

  private FieldComparator<?> getDoubleComparator(int numHits) {
    return new DoubleComparator(
        numHits, getField(), (Double) missingValue, getReverse(), Pruning.NONE) {
      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new DoubleLeafComparator(context) {
          @Override
          protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
              throws IOException {
            SortedNumericDocValues sortedNumeric =
                DocValues.getSortedNumeric(context.reader(), field);
            final BlockJoinSelector.Type type =
                reverseChildren ? BlockJoinSelector.Type.MAX : BlockJoinSelector.Type.MIN;
            final BitSet parents = parentFilter.getBitSet(context);
            final BitSet children = childFilter.getBitSet(context);
            if (children == null) {
              return DocValues.emptyNumeric();
            }
            return new FilterNumericDocValues(
                BlockJoinSelector.wrap(
                    sortedNumeric,
                    type,
                    parents,
                    toIter(children),
                    childMissingValue == null
                        ? null
                        : NumericUtils.doubleToSortableLong((Double) childMissingValue))) {
              @Override
              public long longValue() throws IOException {
                // undo the numericutils sortability
                return NumericUtils.sortableDoubleBits(super.longValue());
              }
            };
          }
        };
      }
    };
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((childFilter == null) ? 0 : childFilter.hashCode());
    result = prime * result + (reverseChildren ? 1231 : 1237);
    result = prime * result + ((parentFilter == null) ? 0 : parentFilter.hashCode());
    result = prime * result + ((childMissingValue == null) ? 0 : childMissingValue.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    ToParentBlockJoinSortField other = (ToParentBlockJoinSortField) obj;
    if (childFilter == null) {
      if (other.childFilter != null) return false;
    } else if (!childFilter.equals(other.childFilter)) return false;
    if (reverseChildren != other.reverseChildren) return false;
    if (parentFilter == null) {
      if (other.parentFilter != null) return false;
    } else if (!parentFilter.equals(other.parentFilter)) return false;
    if (childMissingValue == null) {
      if (other.childMissingValue != null) return false;
    } else if (!childMissingValue.equals(other.childMissingValue)) return false;
    return true;
  }
}
