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
package org.apache.lucene.store;

import java.io.IOException;

/**
 * Optional capability of an {@link IndexInput} whose backing store can fetch many fixed-size float
 * vectors at scattered offsets as one batch, potentially in parallel. Implementations backed by a
 * positional, thread-safe source (for example a file opened with direct I/O) can service the batch
 * concurrently, which the KNN full-precision rerank path uses to read a candidate shortlist without
 * serializing on a single cursor. Because one batch spans the inputs of a whole index, a shortlist
 * scattered over every segment costs one round of reads rather than one per segment. Inputs that do
 * not implement this interface are read serially, a vector at a time.
 *
 * @lucene.experimental
 */
public interface VectorBatchCapable {

  /**
   * Opens a {@link VectorBatch} that can gather reads for this input <em>and</em> for sibling
   * inputs of the same store, so a shortlist spread across many segment files is served by one
   * submission rather than one per file. A batch accepts the inputs it recognises and refuses the
   * rest (see {@link VectorBatch#add}), so a caller reads any refused input by other means.
   */
  VectorBatch newBatch() throws IOException;
}
