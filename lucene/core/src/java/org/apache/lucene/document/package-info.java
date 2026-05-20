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
 * The logical representation of a {@link org.apache.lucene.document.Document} for indexing and
 * searching.
 *
 * <p>The document package provides the user level logical representation of content to be indexed
 * and searched. The package also provides utilities for working with {@link
 * org.apache.lucene.document.Document}s and {@link org.apache.lucene.index.IndexableField}s.
 *
 * <h2>Document and IndexableField</h2>
 *
 * <p>A {@link org.apache.lucene.document.Document} is a collection of {@link
 * org.apache.lucene.index.IndexableField}s. A {@link org.apache.lucene.index.IndexableField} is a
 * logical representation of a user's content that needs to be indexed or stored. {@link
 * org.apache.lucene.index.IndexableField}s have a number of properties that tell Lucene how to
 * treat the content (like indexed, tokenized, stored, etc.) See the {@link
 * org.apache.lucene.document.Field} implementation of {@link
 * org.apache.lucene.index.IndexableField} for specifics on these properties.
 *
 * <p>Note: it is common to refer to {@link org.apache.lucene.document.Document}s having {@link
 * org.apache.lucene.document.Field}s, even though technically they have {@link
 * org.apache.lucene.index.IndexableField}s.
 *
 * <h2>Working with Documents</h2>
 *
 * <p>First and foremost, a {@link org.apache.lucene.document.Document} is something created by the
 * user application. It is your job to create Documents based on the content of the files you are
 * working with in your application (Word, txt, PDF, Excel or any other format.) How this is done is
 * completely up to you. That being said, there are many tools available in other projects that can
 * make the process of taking a file and converting it into a Lucene {@link
 * org.apache.lucene.document.Document}.
 *
 * <h2>How to index ...</h2>
 *
 * <h3>Strings</h3>
 *
 * <p>{@link org.apache.lucene.document.TextField} allows indexing tokens from a String so that one
 * can perform full-text search on it. The way that the input is tokenized depends on the {@link
 * org.apache.lucene.analysis.Analyzer} that is configured on the {@link
 * org.apache.lucene.index.IndexWriterConfig}. TextField can also be optionally stored.
 *
 * <p>{@link org.apache.lucene.document.KeywordField} indexes whole values as a single term so that
 * one can perform exact search on it. It also records doc values to enable sorting or faceting on
 * this field. Finally, it also supports optionally storing the value.
 *
 * <p>If faceting or sorting are not required, {@link org.apache.lucene.document.StringField} is a
 * variant of {@link org.apache.lucene.document.KeywordField} that does not index doc values.
 *
 * <h3>Numbers</h3>
 *
 * <p>If a numeric field represents an identifier rather than a quantity and is more commonly
 * searched on single values than on ranges of values, it is generally recommended to index its
 * string representation via {@link org.apache.lucene.document.KeywordField} (or {@link
 * org.apache.lucene.document.StringField} if doc values are not necessary).
 *
 * <p>{@link org.apache.lucene.document.LongField}, {@link org.apache.lucene.document.IntField},
 * {@link org.apache.lucene.document.DoubleField} and {@link org.apache.lucene.document.FloatField}
 * index values in a points index for efficient range queries, and also create doc-values for these
 * fields for efficient sorting and faceting.
 *
 * <p>If the field is aimed at being used to tune the score, {@link
 * org.apache.lucene.document.FeatureField} helps internally store numeric data as term frequencies
 * in a way that makes it efficient to influence scoring at search time.
 *
 * <h3>Other types of structured data</h3>
 *
 * <p>It is recommended to index dates as a {@link org.apache.lucene.document.LongField} that stores
 * the number of milliseconds since Epoch.
 *
 * <p>IP fields can be indexed via {@link org.apache.lucene.document.InetAddressPoint} in addition
 * to a {@link org.apache.lucene.document.SortedDocValuesField} (if the field is single-valued) or
 * {@link org.apache.lucene.document.SortedSetDocValuesField} that stores the result of {@link
 * org.apache.lucene.document.InetAddressPoint#encode}.
 *
 * <h3>Dense numeric vectors</h3>
 *
 * <p>Dense numeric vectors can be indexed with {@link
 * org.apache.lucene.document.KnnFloatVectorField} if its dimensions are floating-point numbers or
 * {@link org.apache.lucene.document.KnnByteVectorField} if its dimensions are bytes. This allows
 * searching for nearest neighbors at search time.
 *
 * <h3>Sparse numeric vectors</h3>
 *
 * <p>To perform nearest-neighbor search on sparse vectors rather than dense vectors, each dimension
 * of the sparse vector should be indexed as a {@link org.apache.lucene.document.FeatureField}.
 * Queries can then be constructed as a {@link org.apache.lucene.search.BooleanQuery} with {@link
 * org.apache.lucene.document.FeatureField#newLinearQuery(String, String, float) linear queries} as
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 */
package org.apache.lucene.document;
