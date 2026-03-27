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
 * Core utility classes used throughout Lucene.
 *
 * <p>This package provides low-level helper utilities that support many parts of the Lucene
 * indexing and search infrastructure. These classes are generally not intended for direct use by
 * most applications, but may be useful for advanced integrations or extensions.
 *
 * <p>The utilities include:
 *
 * <ul>
 *   <li>Data structures optimized for performance and memory usage
 *   <li>Bit set and numeric helpers
 *   <li>String and byte sequence abstractions such as {@link org.apache.lucene.util.BytesRef}
 *   <li>Reusable iterators and collection helpers
 *   <li>Math and hashing utilities used internally by indexing and search components
 * </ul>
 *
 * <p>APIs in this package are primarily internal and may change between releases.
 */
package org.apache.lucene.util;
