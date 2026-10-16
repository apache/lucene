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
 * ivfaster: an IVF float-vector codec whose clustering is folded into the segment lifecycle.
 *
 * <p>Documents are clustered into {@code nlist} Voronoi cells. A query selects a few cells, scans
 * their documents with a cheap coarse code, and reranks a shortlist with a more precise one. The
 * defaults on {@link org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat} target
 * absolute latency at a fixed recall.
 *
 * <h2>Clustering in the segment lifecycle</h2>
 *
 * <p>There is no separate training step. Clustering is a handful of Lloyd iterations run in place
 * at flush and merge, warm-started from the centroids the largest incoming segment already holds.
 * The cost is amortised into a merge that was going to happen anyway, every merge converges
 * further, and no model ships alongside the index.
 *
 * <h2>The two tiers</h2>
 *
 * <p>The coarse tier is <b>Nitrox2</b>, a 2-bit thermometer code whose symmetric XOR + popcount
 * Hamming distance equals the summed per-dimension level distance, so the planes score as one
 * contiguous popcount over the concatenated code, with no fusion step and no lookup table. It
 * decides which documents survive to reranking.
 *
 * <p>The fine tier is per-dimension <b>int8</b>, scored by an unsigned integer dot with an exact
 * algebraic offset correction, so the recovered signed dot is bit-identical to a signed kernel's.
 * It decides the final ranking.
 *
 * <p>Both kernels are pure-Java Panama with scalar fallbacks, selected at runtime and loaded
 * reflectively, so the codec works on a JVM started without {@code --add-modules
 * jdk.incubator.vector}. There is no native code.
 *
 * <h2>Boundary documents</h2>
 *
 * <p>An IVF index's recall is bounded by whether the right cell was probed at all. Two mechanisms
 * address that: documents near a cell boundary are written into more than one cell (spill, chosen
 * by the SOAR objective), and cell selection descends a small HNSW graph over the centroids.
 * Index-time routing is always an exhaustive scan, so placement quality is paid for once at build.
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.codecs.ivfaster;
