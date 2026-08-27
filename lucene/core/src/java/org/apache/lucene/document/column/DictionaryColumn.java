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
package org.apache.lucene.document.column;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.List;
import java.util.Objects;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link Column} that provides string or binary values via a pre-defined term dictionary plus
 * per-doc ordinals into that dictionary. Used for {@link
 * org.apache.lucene.index.DocValuesType#SORTED SORTED} and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_SET SORTED_SET} doc values, for stored binary or
 * string fields, and for term inversion (tokenized or untokenized).
 *
 * <p>Iteration is performed via cursors. {@link #tuples()} is always available and yields {@code
 * (docID, ordinal)} pairs. {@link #values()} is a bulk cursor over consecutive doc-ids; it must be
 * overridden when {@link #density()} is {@link Column.Density#DENSE DENSE} and is only consulted in
 * that case.
 *
 * <p>The caller supplies a fixed {@code List<BytesRef> dictionary} at construction. Per-doc
 * ordinals returned by cursors index into this dictionary.
 *
 * <p>Duplicate dictionary entries are permitted; two slots with the same bytes will both resolve to
 * the same Lucene-level ordinal. The dictionary may be in any order.
 *
 * <p>The dictionary list and the backing byte arrays of its entries must not be mutated after the
 * column is constructed.
 *
 * @lucene.experimental
 */
public abstract class DictionaryColumn extends Column {

  private final List<BytesRef> dictionary;

  /**
   * Creates a DictionaryColumn.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   * @param density whether every batch-local doc-id has a value
   * @param dictionary the term universe; entries must be non-null and no longer than {@code
   *     ByteBlockPool.BYTE_BLOCK_SIZE - 2}. Must contain at least one entry. Duplicate entries are
   *     allowed but incur a minor per-batch cost.
   */
  protected DictionaryColumn(
      String name, IndexableFieldType fieldType, Density density, List<BytesRef> dictionary) {
    super(name, fieldType, density);
    Objects.requireNonNull(dictionary, "dictionary must not be null");
    if (dictionary.isEmpty()) {
      throw new IllegalArgumentException(
          "DictionaryColumn \"" + name + "\": dictionary must not be empty");
    }
    for (int i = 0; i < dictionary.size(); i++) {
      BytesRef entry = dictionary.get(i);
      if (entry == null) {
        throw new IllegalArgumentException(
            "DictionaryColumn \"" + name + "\": dictionary entry at index " + i + " is null");
      }
      if (entry.length > (BYTE_BLOCK_SIZE - 2)) {
        throw new IllegalArgumentException(
            "DictionaryColumn \""
                + name
                + "\": dictionary entry at index "
                + i
                + " is too large, must be <= "
                + (BYTE_BLOCK_SIZE - 2));
      }
    }
    this.dictionary = dictionary;
  }

  /**
   * Returns the term dictionary. The list is indexed by ordinal; cursors must produce values in
   * {@code [0, dictionary().size())}.
   */
  public final List<BytesRef> dictionary() {
    return dictionary;
  }

  /**
   * Returns a fresh tuple cursor starting at the beginning of the batch. Always available,
   * regardless of {@link #density()}.
   */
  public abstract OrdinalsTupleCursor tuples();

  /**
   * Returns a fresh dense cursor for doc-ids {@code [0, numDocs)}, producing exactly one ordinal
   * per doc. Must be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE}; the
   * default implementation throws {@link UnsupportedOperationException} and is never called for
   * {@link Column.Density#SPARSE SPARSE} columns.
   */
  public OrdinalsCursor values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }

  /**
   * The stored-field type emitted for this column. The default is {@link
   * org.apache.lucene.document.StoredValue.Type#BINARY}. Only {@link
   * org.apache.lucene.document.StoredValue.Type#BINARY} and {@link
   * org.apache.lucene.document.StoredValue.Type#STRING} are supported; subclasses may override to
   * emit string stored values.
   */
  public StoredValue.Type storedType() {
    return StoredValue.Type.BINARY;
  }
}
