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
 * Binary I/O API used for all of Lucene's index data.
 *
 * <p>This package provides the storage layer on top of which every Lucene index is built. All index
 * artifacts (postings, stored fields, term vectors, doc values, points and kNN vectors) are
 * persisted and read back through a {@link org.apache.lucene.store.Directory} using the binary
 * stream abstractions defined here.
 *
 * <h2>Directory</h2>
 *
 * <p>The central entry point of this package is {@link org.apache.lucene.store.Directory}, an
 * abstract directory of named files. {@link org.apache.lucene.store.Directory#openInput(String,
 * org.apache.lucene.store.IOContext)} opens an existing file for reading, while {@link
 * org.apache.lucene.store.Directory#createOutput(String,
 * org.apache.lucene.store.IOContext)} creates a new file for writing. The concrete implementation of
 * {@link org.apache.lucene.store.Directory} describes where the files physically live:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.store.FSDirectory} (and its optimized subclass {@link
 *       org.apache.lucene.store.MMapDirectory}) stores files on the local file system.
 *   <li>{@link org.apache.lucene.store.ByteBuffersDirectory} keeps files in off-heap memory and is
 *       primarily used internally.
 *   <li>Base classes such as {@link org.apache.lucene.store.FilterDirectory} and {@link
 *       org.apache.lucene.store.TrackingDirectoryWrapper} make it easy to add behaviour around an
 *       existing directory.
 * </ul>
 *
 * <h2>Reading and writing</h2>
 *
 * <p>Reading and writing are performed via {@link org.apache.lucene.store.IndexInput} and {@link
 * org.apache.lucene.store.IndexOutput}, whose low-level primitives are provided by the abstract
 * {@link org.apache.lucene.store.DataInput} and {@link org.apache.lucene.store.DataOutput} classes.
 * These streams support the primitive integer and long encodings that Lucene's codecs rely on, and
 * {@link org.apache.lucene.store.ChecksumIndexInput} reports a checksum over the bytes read so a
 * caller can verify that a file was not corrupted.
 *
 * <h2>Locking and consistency</h2>
 *
 * <p>To make sure only one process writes a given directory at a time, the package exposes {@link
 * org.apache.lucene.store.Lock} and its factories (for example {@link
 * org.apache.lucene.store.SimpleFSLockFactory} or {@link org.apache.lucene.store.NativeFSLockFactory})
 * through {@link org.apache.lucene.store.Directory#obtainLock(String)}.
 *
 * <p>See {@link org.apache.lucene.index.IndexWriter} in the {@code org.apache.lucene.index}
 * package for the canonical example of using this package to build and maintain an index.
 */
package org.apache.lucene.store;
