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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.comparators.TermOrdValComparator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * SortField for {@link SortedSetDocValues}.
 *
 * <p>A SortedSetDocValues contains multiple values for a field, so sorting with this technique
 * "selects" a value as the representative sort value for the document.
 *
 * <p>By default, the minimum value in the set is selected as the sort value, but this can be
 * customized. Selectors other than the default do have some limitations to ensure that all
 * selections happen in constant-time for performance.
 *
 * <p>Like sorting by string, this also supports sorting missing values as first or last, via {@link
 * SortField#STRING_FIRST} or {@link SortField#STRING_LAST}.
 *
 * @see SortedSetSelector
 */
public class SortedSetSortField extends SortField {

  private final SortedSetSelector.Type selector;

  /**
   * Creates a sort, possibly in reverse, by the minimum value in the set for the document.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   */
  public SortedSetSortField(String field, boolean reverse) {
    this(field, reverse, SortedSetSelector.Type.MIN, null);
  }

  public SortedSetSortField(String field, boolean reverse, SortedSetSelector.Type selector) {
    this(field, reverse, selector, null);
  }

  /**
   * Creates a sort, possibly in reverse, specifying how the sort value from the document's set is
   * selected.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   *     <p>NOTE: selectors other than {@link SortedSetSelector.Type#MIN} require optional codec
   *     support.
   */
  public SortedSetSortField(
      String field, boolean reverse, SortedSetSelector.Type selector, Object missingValue) {
    super(field, SortField.Type.CUSTOM, reverse, missingValue);
    if (selector == null) {
      throw new NullPointerException();
    }
    this.selector = selector;
  }

  /** A SortFieldProvider for this sort */
  public static final class Provider extends SortFieldProvider {

    /** The name this provider is registered under */
    public static final String NAME = "SortedSetSortField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public SortField readSortField(DataInput in) throws IOException {
      String field = in.readString();
      boolean reverse = in.readInt() == 1;
      SortedSetSelector.Type type = readSelectorType(in);
      int missingValue = in.readInt();
      if (missingValue == 1) {
        return new SortedSetSortField(field, reverse, type, SortField.STRING_FIRST);
      } else if (missingValue == 2) {
        return new SortedSetSortField(field, reverse, type, SortField.STRING_LAST);
      }
      return new SortedSetSortField(field, reverse, type, null);
    }

    @Override
    public void writeSortField(SortField sf, DataOutput out) throws IOException {
      assert sf instanceof SortedSetSortField;
      ((SortedSetSortField) sf).serialize(out);
    }
  }

  private static SortedSetSelector.Type readSelectorType(DataInput in) throws IOException {
    int type = in.readInt();
    if (type >= SortedSetSelector.Type.values().length) {
      throw new IllegalArgumentException(
          "Cannot deserialize SortedSetSortField: unknown selector type " + type);
    }
    return SortedSetSelector.Type.values()[type];
  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(getField());
    out.writeInt(reverse ? 1 : 0);
    out.writeInt(selector.ordinal());
    if (missingValue == SortField.STRING_FIRST) {
      out.writeInt(1);
    } else if (missingValue == SortField.STRING_LAST) {
      out.writeInt(2);
    } else {
      out.writeInt(0);
    }
  }

  /** Returns the selector in use for this sort */
  public SortedSetSelector.Type getSelector() {
    return selector;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + selector.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    SortedSetSortField other = (SortedSetSortField) obj;
    if (selector != other.selector) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<sortedset" + ": \"").append(getField()).append("\">");
    if (getReverse()) buffer.append('!');
    if (missingValue != null) {
      buffer.append(" missingValue=");
      buffer.append(missingValue);
    }
    buffer.append(" selector=");
    buffer.append(selector);

    return buffer.toString();
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    Pruning finalPruning = getOptimizeSortWithIndexedData() ? pruning : Pruning.NONE;
    return new TermOrdValComparator(
        numHits, getField(), missingValue == STRING_LAST, reverse, finalPruning) {
      @Override
      protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
          throws IOException {
        return SortedSetSelector.wrap(DocValues.getSortedSet(context.reader(), field), selector);
      }
    };
  }

  private SortedDocValues getValues(LeafReader reader) throws IOException {
    return SortedSetSelector.wrap(DocValues.getSortedSet(reader, getField()), selector);
  }

  @Override
  public IndexSorter getIndexSorter() {
    return new IndexSorter.StringSorter(Provider.NAME, missingValue, reverse, this::getValues);
  }
}
