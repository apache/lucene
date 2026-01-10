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
 * Hierarchical Navigable Small-World (HNSW) graph implementation for efficient approximate nearest
 * neighbor search.
 *
 * <p>This package provides a fully hierarchical HNSW implementation with optional flat mode support.
 * When flatMode is enabled, all nodes are placed on level 0, which can provide memory savings
 * (approximately 38%) for high-dimensional vectors (d >= 32) without sacrificing recall or latency.
 *
 * <p>The flat mode is based on research showing that high-dimensional spaces naturally form "hub
 * highways" that provide the same fast routing as hierarchical layers. For more details, see:
 * "Down with the Hierarchy: The 'H' in HNSW Stands for 'Hubs'" (arXiv:2412.01940).
 *
 * <p>Use hierarchical mode (default) for low-dimensional vectors (d < 32) and flat mode for
 * high-dimensional vectors to optimize memory usage.
 */
package org.apache.lucene.util.hnsw;
