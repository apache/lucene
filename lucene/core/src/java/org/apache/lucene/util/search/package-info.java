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
 * Utility classes and methods for advanced search operations in Lucene.
 *
 * <p>The {@code org.apache.lucene.util.search} package provides reusable helpers for point-based
 * queries, including validation and spatial relation logic for multi-dimensional point fields.
 * These utilities are intended to support efficient and correct implementation of range queries and
 * related search features in Lucene.
 *
 * <p>Classes in this package are typically used by core search components such as {@link
 * org.apache.lucene.search.PointRangeQuery} and {@link org.apache.lucene.search.PointRangeWeight}.
 *
 * @since 9.0
 */
package org.apache.lucene.util.search;
