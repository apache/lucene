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

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;

/** Abstract base class implementing a {@link KnnVectorsReader} that has no vector values. */
public abstract class EmptyKnnVectorsReader extends KnnVectorsReader {
  /** Sole constructor */
  protected EmptyKnnVectorsReader() {}

  @Override
  public void checkIntegrity() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TopDocs search(String field, float[] target, int k, Bits acceptDocs) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
