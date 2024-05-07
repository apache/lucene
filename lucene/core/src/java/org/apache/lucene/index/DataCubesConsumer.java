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
package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;

/**
 * Abstract API to write DataCubeIndices
 *
 * @lucene.experimental
 */
public abstract class DataCubesConsumer implements Closeable {

  /** Sole constructor */
  public DataCubesConsumer() throws IOException {
    super();
  }

  /** Create the DataCubes index based on the DocValues and flushes the indices to disk */
  public abstract void flush(
      SegmentWriteState state,
      DataCubesConfig dataCubesConfig,
      LeafReader docValuesReader,
      Sorter.DocMap sortMap)
      throws IOException;

  /** Merges in the DataCubes fields from the readers in <code>mergeState</code>. */
  public abstract void merge(MergeState mergeState) throws IOException;

  @Override
  public void close() throws IOException {}
}
