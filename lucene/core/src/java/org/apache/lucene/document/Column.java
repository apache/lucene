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
package org.apache.lucene.document;

import java.util.Objects;
import org.apache.lucene.index.IndexableFieldType;

/**
 * A single field's values across multiple documents in a {@link ColumnBatch}. A Column carries only
 * metadata (name and field type); iteration is performed via cursors obtained from {@link
 * LongColumn} or {@link BinaryColumn}.
 *
 * <p>Each call that requests a cursor returns a fresh cursor positioned at the first value, so
 * columns can be consumed multiple times (for example, once in the row-oriented pass for stored
 * fields and again in the column-oriented pass for doc values).
 *
 * @lucene.experimental
 */
public abstract class Column {

  private final String name;
  private final IndexableFieldType fieldType;

  /**
   * Creates a Column with the given field name and type.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   */
  protected Column(String name, IndexableFieldType fieldType) {
    this.name = Objects.requireNonNull(name, "field name must not be null");
    this.fieldType = Objects.requireNonNull(fieldType, "field type must not be null");
  }

  /** Returns the field name. */
  public String name() {
    return name;
  }

  /** Returns the field type describing how this field is indexed. */
  public IndexableFieldType fieldType() {
    return fieldType;
  }
}
