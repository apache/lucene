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
 * TurboQuant vector quantization codec for Apache Lucene.
 *
 * <p>Implements the TurboQuant algorithm (Zandieh et al., ICLR 2026) as a {@link
 * org.apache.lucene.codecs.hnsw.FlatVectorsFormat} for near-optimal data-oblivious vector
 * quantization.
 *
 * <h2>Algorithm</h2>
 *
 * <ol>
 *   <li>Store original norm {@code ||x||} as float32
 *   <li>Normalize: {@code x̂ = x / ||x||}
 *   <li>Random rotation: {@code y = Π · x̂} (shared globally via deterministic seed)
 *   <li>Scalar quantize each coordinate using precomputed Beta-distribution-optimal Lloyd-Max
 *       centroids → b-bit index per coordinate
 * </ol>
 *
 * <h2>File Format</h2>
 *
 * <table>
 * <caption>TurboQuant file extensions</caption>
 * <tr><th>Extension</th><th>Contents</th></tr>
 * <tr><td>{@code .vetq}</td><td>Packed b-bit indices + float32 norms, contiguous per-doc, off-heap</td></tr>
 * <tr><td>{@code .vemtq}</td><td>Metadata: dimension, encoding, vector count, rotation seed, similarity</td></tr>
 * </table>
 *
 * <p>Raw vectors ({@code .vec}) and HNSW graph ({@code .vex}) are delegated to existing formats.
 *
 * <h2>When to Use TurboQuant</h2>
 *
 * <ul>
 *   <li>High-dimensional embeddings (d=4096 or higher) — exceeds 1024-dim limit of scalar quant
 *   <li>Data distribution shifts over time — no recalibration needed (data-oblivious)
 *   <li>Streaming/online indexing — each vector quantized independently
 *   <li>Merge-heavy workloads — byte-copy merge (no re-quantization)
 * </ul>
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li>Minimum dimension: 32 (Gaussian approximation requires sufficient d)
 *   <li>Float32 input only (no byte vector support)
 *   <li>Maximum dimension: 16384
 * </ul>
 *
 * @see org.apache.lucene.sandbox.codecs.turboquant.TurboQuantHnswVectorsFormat
 * @see org.apache.lucene.sandbox.codecs.turboquant.TurboQuantFlatVectorsFormat
 * @see org.apache.lucene.sandbox.codecs.turboquant.TurboQuantEncoding
 */
package org.apache.lucene.sandbox.codecs.turboquant;
