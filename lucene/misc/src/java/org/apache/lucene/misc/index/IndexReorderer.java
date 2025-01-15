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
package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;

/** Interface for docid-reordering expected by {@link BPReorderingMergePolicy}. */
public interface IndexReorderer {
  /**
   * Returns a mapping from old to new docids.
   *
   * @param reader the reader whose docs are to be reordered
   * @param tempDir temporary files may be stored here while reordering.
   * @param executor may be used to parallelize reordering work.
   */
  Sorter.DocMap computeDocMap(CodecReader reader, Directory tempDir, Executor executor)
      throws IOException;
}
