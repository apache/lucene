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

/**
 * Column-oriented batch indexing API.
 *
 * <p>{@link
 * org.apache.lucene.index.IndexWriter#addBatch(org.apache.lucene.document.column.ColumnBatch)}
 * accepts a {@link org.apache.lucene.document.column.ColumnBatch}: a fixed number of documents
 * presented field-by-field rather than document-by-document. Each field is a {@link
 * org.apache.lucene.document.column.Column} that exposes its values via cursor iterators rather
 * than concrete {@link org.apache.lucene.index.IndexableField} instances per document.
 *
 * <h2>Column subtypes</h2>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.document.column.LongColumn} — single- or multi-valued long values
 *       for {@link org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} / {@link
 *       org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values, 1‑D
 *       numeric points (int / long / float / double), and stored numeric fields.
 *   <li>{@link org.apache.lucene.document.column.BinaryColumn} — variable-length binary values for
 *       {@link org.apache.lucene.index.DocValuesType#BINARY BINARY}, {@link
 *       org.apache.lucene.index.DocValuesType#SORTED SORTED}, and {@link
 *       org.apache.lucene.index.DocValuesType#SORTED_SET SORTED_SET} doc values, term inversion,
 *       multi-dimensional or arbitrary-width points, and stored binary or string fields.
 *   <li>{@link org.apache.lucene.document.column.VectorColumn} — KNN vectors (FLOAT32 or BYTE
 *       encoding); vector-only field type.
 * </ul>
 *
 * <h2>Cursors</h2>
 *
 * <p>A {@link org.apache.lucene.document.column.Column} declares its {@link
 * org.apache.lucene.document.column.Column.Density} (DENSE or SPARSE) and exposes its values via
 * cursors:
 *
 * <ul>
 *   <li>A tuple cursor (e.g. {@link org.apache.lucene.document.column.LongTupleCursor}, {@link
 *       org.apache.lucene.document.column.ObjectTupleCursor}) yields {@code (batchDocID, value)}
 *       pairs in non-decreasing doc-id order. Always available.
 *   <li>A bulk values cursor (e.g. {@link org.apache.lucene.document.column.LongValuesCursor})
 *       feeds dense data directly into the underlying writer. Required when {@link
 *       org.apache.lucene.document.column.Column#density()} is {@link
 *       org.apache.lucene.document.column.Column.Density#DENSE DENSE} and consulted only in that
 *       case.
 * </ul>
 *
 * <p>Each call that requests a cursor returns a fresh cursor positioned at the first value, so
 * columns can be consumed multiple times — once in the row-oriented pass for stored fields and term
 * inversion, and again in the column-oriented pass for doc values, points, and vectors.
 *
 * @lucene.experimental
 */
package org.apache.lucene.document.column;
