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

package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Encodes/decodes per-document vectors and provides a scoring interface for the flat stored vectors
 *
 * @lucene.experimental
 */
public abstract class FlatVectorsFormat extends KnnVectorsFormat {

  /** Sole constructor */
  protected FlatVectorsFormat(String name) {
    super(name);
  }

  /** Returns a {@link FlatVectorsWriter} to write the vectors to the index. */
  @Override
  public abstract FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /** Returns a {@link KnnVectorsReader} to read the vectors from the index. */
  @Override
  public abstract FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException;

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }
}
