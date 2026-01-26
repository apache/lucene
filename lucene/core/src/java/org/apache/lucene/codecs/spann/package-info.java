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
 * SPANN: Disk-Resident HNSW-IVF Vectors Format.
 *
 * <p>This package contains the implementation of the SPANN vector format, which implements a
 * "Disk-Resident HNSW-IVF" architecture:
 *
 * <ul>
 *   <li><b>Navigation (Tier 1)</b>: Centroids are indexed using {@link
 *       org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat}. This allows efficient
 *       off-heap navigation of millions of centroids.
 *   <li><b>Storage (Tier 2)</b>: Data vectors are stored in a clustered, sequential format on disk,
 *       optimized for large-scale scanning.
 * </ul>
 *
 * <p>The main entry point is {@link org.apache.lucene.codecs.spann.Lucene99SpannVectorsFormat}.
 */
package org.apache.lucene.codecs.spann;
