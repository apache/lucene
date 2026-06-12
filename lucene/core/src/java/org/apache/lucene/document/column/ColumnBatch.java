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

/**
 * A column-oriented batch of documents for indexing. A Batch contains a collection of {@link
 * Column}s, where each Column represents a single field across all documents in the batch.
 * Documents are identified by batch-local IDs from 0 (inclusive) to {@link #numDocs()} (exclusive).
 *
 * @lucene.experimental
 */
public abstract class ColumnBatch {

  /**
   * Returns the number of documents in this batch. All column doc-ids must be in the range [0,
   * numDocs()).
   */
  public abstract int numDocs();

  /**
   * Returns the columns in this batch. Each column covers one indexing feature (inversion, stored,
   * doc values, points, or vectors) of a field across all documents in the batch.
   *
   * <p>Multiple columns may share a field name to combine distinct features — for example, a stored
   * {@link BinaryColumn} alongside a separate inverted column for the same field — but each feature
   * must be carried by exactly one column. Multi-valued fields must use a single column whose tuple
   * cursor emits multiple values per doc-id; a feature must not be split across columns.
   */
  public abstract Iterable<Column> columns();
}
