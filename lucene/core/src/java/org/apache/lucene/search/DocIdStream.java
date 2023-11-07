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
package org.apache.lucene.search;

import java.io.IOException;

/**
 * A stream of doc IDs. Most methods on {@link DocIdStream}s are terminal, meaning that the {@link
 * DocIdStream} may not be further used.
 *
 * @see LeafCollector#collect(DocIdStream)
 * @lucene.experimental
 */
public abstract class DocIdStream {

  /** Sole constructor, for invocation by sub classes. */
  protected DocIdStream() {}

  /**
   * Iterate over doc IDs contained in this stream in order, calling the given {@link
   * CheckedIntConsumer} on them. This is a terminal operation.
   */
  public abstract void forEach(CheckedIntConsumer<IOException> consumer) throws IOException;

  /** Count the number of entries in this stream. This is a terminal operation. */
  public int count() throws IOException {
    int[] count = new int[1];
    forEach(doc -> count[0]++);
    return count[0];
  }
}
