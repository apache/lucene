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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;

/**
 * A {@link Column} that provides caller-supplied {@link TokenStream}s for term inversion. This is
 * the columnar analogue of setting a custom {@link TokenStream} on a {@link
 * org.apache.lucene.document.Field}: the per-doc tokens are fed straight to the inverter without
 * going through an {@link org.apache.lucene.analysis.Analyzer}.
 *
 * <p>Token-stream columns are inverted-index-only: the field type must declare {@code
 * indexOptions() != NONE} and {@code tokenized() == true}, and must not also set doc values,
 * points, be stored, or declare vectors. To both store a value and invert a custom token stream
 * under one field name, add a separate stored {@link BinaryColumn} (or {@link LongColumn}) with the
 * same name in the same batch — each column carries a distinct indexing aspect.
 *
 * @lucene.experimental
 */
public abstract class TokenStreamColumn extends Column {

  /**
   * Creates a TokenStreamColumn with the given field name, type, and density.
   *
   * @throws IllegalArgumentException if {@code fieldType} is not indexed or not tokenized
   */
  protected TokenStreamColumn(String name, IndexableFieldType fieldType, Density density) {
    super(name, fieldType, density);
    if (fieldType.indexOptions() == IndexOptions.NONE || fieldType.tokenized() == false) {
      throw new IllegalArgumentException(
          "TokenStreamColumn \""
              + name
              + "\" requires fieldType.indexOptions() != NONE and fieldType.tokenized() == true; got"
              + " indexOptions="
              + fieldType.indexOptions()
              + ", tokenized="
              + fieldType.tokenized());
    }
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract ObjectTupleCursor<TokenStream> tuples();
}
