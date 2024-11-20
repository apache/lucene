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
 * HNSW vector helper classes. The classes in this package provide a scoring and storing mechanism
 * for vectors stored in a flat file. This allows for HNSW formats to be extended with other flat
 * storage formats or scoring without significant changes to the HNSW code. Some examples for
 * scoring include {@link org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer} and {@link
 * org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer}. Some examples for storing include {@link
 * org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat} and {@link
 * org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat}.
 */
package org.apache.lucene.codecs.hnsw;
