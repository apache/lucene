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
import java.util.Comparator;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * SortField for {@link BinaryDocValues}, usable as an <b>index sort</b>.
 *
 * <p>Unlike {@link SortField.Type#STRING} (backed by {@link org.apache.lucene.index.SortedDocValues
 * SortedDocValues}) or {@link SortedSetSortField}, which deduplicate values into a term dictionary
 * and are best suited to low-cardinality fields, this sort compares the raw bytes of a {@link
 * BinaryDocValues} field directly. It is therefore a good fit for high-cardinality fields where a
 * term dictionary would be pure overhead.
 *
 * <p>By default documents are ordered by the unsigned byte order of their value ({@link
 * BytesRef#compareTo}). When the stored bytes are not directly comparable — for example when the
 * value is encoded and only part of it is the sort key — a custom {@link Comparator} that decodes
 * the value can be supplied by subclassing and passing it to {@link #BinarySortField(String,
 * boolean, Object, Comparator, String)}. The subclass must register a companion {@link
 * SortFieldProvider} under {@code META-INF/services} so the comparator is reconstructed when the
 * index sort is read back, and therefore used on merge.
 *
 * <p>Missing values are sorted first or last via {@link SortField#STRING_FIRST} or {@link
 * SortField#STRING_LAST} (the default, when unset, is to sort missing values first).
 *
 * <p>This {@link SortField} can also be used to sort search results, in which case it compares
 * values with a linear scan of the binary doc values (it does not benefit from the skipping
 * optimizations available to numeric or {@code SORTED} fields).
 *
 * @lucene.experimental
 */
public class BinarySortField extends SortField {

  /** The default unsigned byte order comparator. */
  static final Comparator<BytesRef> DEFAULT_COMPARATOR = Comparator.naturalOrder();

  private final Comparator<BytesRef> bytesComparator;
  private final String providerName;

  /**
   * Creates a sort, possibly in reverse, by the unsigned byte order of the field's value.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   */
  public BinarySortField(String field, boolean reverse) {
    this(field, reverse, null);
  }

  /**
   * Creates a sort, possibly in reverse, by the unsigned byte order of the field's value, with the
   * given handling of missing values.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   * @param missingValue {@link SortField#STRING_FIRST}, {@link SortField#STRING_LAST}, or {@code
   *     null} (missing first).
   */
  public BinarySortField(String field, boolean reverse, Object missingValue) {
    this(field, reverse, missingValue, DEFAULT_COMPARATOR, Provider.NAME);
  }

  /**
   * Expert: creates a sort with a custom value comparator.
   *
   * <p>Because the comparator cannot be serialized, subclasses using this constructor must supply
   * the name of a dedicated {@link SortFieldProvider} (registered via SPI) whose {@link
   * SortFieldProvider#readSortField} reconstructs a {@code BinarySortField} configured with the
   * same comparator. The comparator is thus identified by the provider name rather than serialized.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   * @param missingValue {@link SortField#STRING_FIRST}, {@link SortField#STRING_LAST}, or {@code
   *     null}.
   * @param bytesComparator comparator applied to present (non-missing) values.
   * @param providerName the SPI name of the companion {@link SortFieldProvider}.
   */
  protected BinarySortField(
      String field,
      boolean reverse,
      Object missingValue,
      Comparator<BytesRef> bytesComparator,
      String providerName) {
    super(field, SortField.Type.CUSTOM, reverse, missingValue);
    if (missingValue != null && missingValue != STRING_FIRST && missingValue != STRING_LAST) {
      throw new IllegalArgumentException(
          "missing value for BinarySortField must be null, STRING_FIRST or STRING_LAST");
    }
    if (bytesComparator == null) {
      throw new NullPointerException("bytesComparator must not be null");
    }
    if (providerName == null) {
      throw new NullPointerException("providerName must not be null");
    }
    this.bytesComparator = bytesComparator;
    this.providerName = providerName;
  }

  /** A SortFieldProvider for this sort */
  public static final class Provider extends SortFieldProvider {

    /** The name this provider is registered under */
    public static final String NAME = "BinarySortField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public SortField readSortField(DataInput in) throws IOException {
      String field = in.readString();
      boolean reverse = in.readInt() == 1;
      int missingValue = in.readInt();
      if (missingValue == 1) {
        return new BinarySortField(field, reverse, SortField.STRING_FIRST);
      } else if (missingValue == 2) {
        return new BinarySortField(field, reverse, SortField.STRING_LAST);
      }
      return new BinarySortField(field, reverse);
    }

    @Override
    public void writeSortField(SortField sf, DataOutput out) throws IOException {
      assert sf instanceof BinarySortField;
      ((BinarySortField) sf).serialize(out);
    }
  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(getField());
    out.writeInt(reverse ? 1 : 0);
    if (missingValue == SortField.STRING_FIRST) {
      out.writeInt(1);
    } else if (missingValue == SortField.STRING_LAST) {
      out.writeInt(2);
    } else {
      out.writeInt(0);
    }
  }

  private BinaryDocValues getValues(LeafReader reader) throws IOException {
    return DocValues.getBinary(reader, getField());
  }

  /**
   * The value comparator extended to treat a {@code null} argument as a missing value, ordered
   * first or last according to {@link #missingValue}. The {@code reverse} flag is applied by the
   * caller (so that, like {@link SortField.Type#STRING}, missing values follow {@code reverse}),
   * never here.
   */
  private Comparator<BytesRef> comparator() {
    final boolean missingLast = missingValue == SortField.STRING_LAST;
    final Comparator<BytesRef> valueComparator = bytesComparator;
    return (a, b) -> {
      if (a == null || b == null) {
        if (a == b) {
          return 0;
        }
        int c = (a == null) ? -1 : 1;
        return missingLast ? -c : c;
      }
      return valueComparator.compare(a, b);
    };
  }

  @Override
  public IndexSorter getIndexSorter() {
    return new IndexSorter.BinarySorter(providerName, reverse, comparator(), this::getValues);
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    final Comparator<BytesRef> comparator = comparator();
    // Reuse the generic binary-doc-values comparator, plugging in the same comparison used by the
    // index sorter so the two orderings stay consistent.
    return new FieldComparator.TermValComparator(
        numHits, getField(), missingValue == SortField.STRING_LAST) {
      @Override
      public int compareValues(BytesRef val1, BytesRef val2) {
        return comparator.compare(val1, val2);
      }
    };
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + providerName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BinarySortField other = (BinarySortField) obj;
    return providerName.equals(other.providerName);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<binary: \"").append(getField()).append("\">");
    if (getReverse()) {
      buffer.append('!');
    }
    if (missingValue != null) {
      buffer.append(" missingValue=");
      buffer.append(missingValue);
    }
    return buffer.toString();
  }
}
