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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * SortField for {@link BinaryDocValues}, usable as an <b>index sort</b>.
 *
 * <p>Unlike a {@link SortField.Type#STRING} sort (backed by {@link
 * org.apache.lucene.index.SortedDocValues SortedDocValues}), values are compared directly rather
 * than through a term dictionary, making this a good fit for high-cardinality fields where a
 * dictionary would be pure overhead. Documents are ordered by the unsigned byte order of their sort
 * key ({@link BytesRef#compareTo}), optionally reversed, with missing values sorted first or last
 * via {@link SortField#STRING_FIRST} / {@link SortField#STRING_LAST} (first by default).
 *
 * <p>By default the sort key is the stored value. A subclass may override {@link
 * #getSortKeyDocValues(LeafReader)} to derive an order-preserving key from the stored bytes and/or
 * other doc-values fields of the segment (for example to select the minimum of a multi-valued
 * field). Such a subclass must register a {@link SortFieldProvider} via SPI so the sort can be read
 * back and used on merge.
 *
 * <p>This field can also sort search results, comparing values with a linear scan of the binary doc
 * values (without the skipping optimizations available to numeric or {@code SORTED} fields).
 *
 * @lucene.experimental
 */
public class BinarySortField extends SortField {

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
    this(field, reverse, missingValue, Provider.NAME);
  }

  /**
   * Creates a sort for a subclass that overrides {@link #getSortKeyDocValues(LeafReader)}.
   *
   * <p>The subclass must supply the name of a dedicated {@link SortFieldProvider} (registered via
   * SPI) whose {@link SortFieldProvider#readSortField} reconstructs the subclass, so that the same
   * sort-key derivation is used when the index sort is read back and on merge.
   *
   * @param field Name of field to sort by. Must not be null.
   * @param reverse True if natural order should be reversed.
   * @param missingValue {@link SortField#STRING_FIRST}, {@link SortField#STRING_LAST}, or {@code
   *     null}.
   * @param providerName the SPI name of the companion {@link SortFieldProvider}.
   */
  protected BinarySortField(
      String field, boolean reverse, Object missingValue, String providerName) {
    super(field, SortField.Type.CUSTOM, reverse, missingValue);
    if (missingValue != null && missingValue != STRING_FIRST && missingValue != STRING_LAST) {
      throw new IllegalArgumentException(
          "missing value for BinarySortField must be null, STRING_FIRST or STRING_LAST");
    }
    if (providerName == null) {
      throw new NullPointerException("providerName must not be null");
    }
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

  /**
   * Returns the per-document sort key, as a {@link BinaryDocValues} whose {@link
   * BinaryDocValues#binaryValue()} is the bytes to sort on. The default returns the field's values
   * unchanged; override to derive a key from the stored bytes and/or other doc-values fields of the
   * segment.
   *
   * <p>Values are read in increasing docID order; advance any underlying doc values with {@link
   * DocIdSetIterator#nextDoc()} only, since at flush time the in-memory doc values do not support
   * {@code advanceExact}. The returned key has the same lifetime as {@link
   * BinaryDocValues#binaryValue()} (valid until the next call) and need not be a copy.
   */
  protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
    return DocValues.getBinary(reader, getField());
  }

  /** Unsigned byte order comparator that treats a {@code null} argument as a missing value. */
  private Comparator<BytesRef> comparator() {
    final boolean missingLast = missingValue == SortField.STRING_LAST;
    return (a, b) -> {
      if (a == null || b == null) {
        if (a == b) {
          return 0;
        }
        int c = (a == null) ? -1 : 1;
        return missingLast ? -c : c;
      }
      return a.compareTo(b);
    };
  }

  @Override
  public IndexSorter getIndexSorter() {
    return new IndexSorter.BinarySorter(
        providerName, reverse, comparator(), this::getSortKeyDocValues);
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
    return new FieldComparator.TermValComparator(
        numHits, getField(), missingValue == SortField.STRING_LAST) {
      @Override
      protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field)
          throws IOException {
        return getSortKeyDocValues(context.reader());
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
