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
 * A postings format that adds a per-segment hash index for exact lookups to primary-key fields:
 * fields whose terms are primary keys or other unique IDs, such as UUIDs, product IDs or URLs, each
 * in one document.
 *
 * <ul>
 *   <li>{@link org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat} is the format.
 *       It is meant to be applied per field, to primary-key fields only.
 *   <li>{@link org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery} matches the documents
 *       that hold any of a set of terms.
 * </ul>
 */
package org.apache.lucene.sandbox.codecs.pkhash;
