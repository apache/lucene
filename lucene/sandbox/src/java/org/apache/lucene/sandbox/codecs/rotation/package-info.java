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
 * A lightweight, data-oblivious rotation preconditioner for float vector fields.
 *
 * <p>The main entry point is {@link
 * org.apache.lucene.sandbox.codecs.rotation.RotationPreconditionedVectorsFormat}. It wraps any
 * other {@link org.apache.lucene.codecs.hnsw.FlatVectorsFormat} — most usefully the optimized
 * scalar quantization format ({@code Lucene104ScalarQuantizedVectorsFormat}) — and applies a
 * randomized Hadamard rotation to vectors before quantization and to queries before scoring. The
 * rotation is orthogonal, so the geometry of the vector space is preserved (dot product, cosine,
 * and Euclidean distance are all unchanged), but the per-coordinate distribution of the vectors
 * becomes much more Gaussian. This makes scalar quantizers that assume Gaussian components work
 * well on datasets where the raw components are skewed or uniform (image pixels, histograms,
 * non-transformer embeddings).
 *
 * <p>See the {@link
 * org.apache.lucene.sandbox.codecs.rotation.RotationPreconditionedVectorsFormat} class Javadoc for
 * a usage example and more detail about cost, properties, and related work.
 */
package org.apache.lucene.sandbox.codecs.rotation;
