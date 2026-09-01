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

import java.util.Objects;
import org.apache.lucene.index.IndexableFieldType;

/**
 * A single field's values across multiple documents in a {@link ColumnBatch}. A Column carries only
 * metadata (name, field type, and density); iteration is performed via cursors obtained from {@link
 * LongColumn}, {@link BinaryColumn}, or {@link VectorColumn}.
 *
 * <p>Each call that requests a cursor returns a fresh cursor positioned at the first value, so
 * columns can be consumed multiple times (for example, once in the row-oriented pass for stored
 * fields and again in the column-oriented pass for doc values).
 *
 * @lucene.experimental
 */
public abstract class Column {

  /**
   * Whether a column has a value for every document in the batch. This is a contract the column
   * asserts up-front so the indexing chain can pick the right code path without probing the data.
   */
  public enum Density {
    /** The column has a value for every batch-local doc-id in {@code [0, numDocs)}, in order. */
    DENSE,
    /** The column may be missing values or have multiple values for some doc-ids. */
    SPARSE,
  }

  private final String name;
  private final IndexableFieldType fieldType;
  private final Density density;

  /**
   * Creates a Column with the given field name, type, and density.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   * @param density whether this column has a value for every document in the batch
   */
  protected Column(String name, IndexableFieldType fieldType, Density density) {
    this.name = Objects.requireNonNull(name, "field name must not be null");
    this.fieldType = Objects.requireNonNull(fieldType, "field type must not be null");
    this.density = Objects.requireNonNull(density, "density must not be null");
  }

  /** Returns the field name. */
  public String name() {
    return name;
  }

  /** Returns the field type describing how this field is indexed. */
  public IndexableFieldType fieldType() {
    return fieldType;
  }

  /** Returns the density of this column (whether every doc has a value). */
  public Density density() {
    return density;
  }
}
