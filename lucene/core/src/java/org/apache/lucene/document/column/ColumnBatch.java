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
   * Returns the columns in this batch. Each column represents a single field across the documents
   * in the batch. Within a batch, column names must be unique: multi-valued fields should be
   * expressed as a single column whose tuple cursor emits multiple values per batch doc-id.
   */
  public abstract Iterable<Column> columns();
}
