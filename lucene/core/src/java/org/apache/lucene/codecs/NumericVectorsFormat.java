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

package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.NumericVectors;

/**
 * Encodes/decodes per-document vector and any associated indexing structures required to support
 * nearest-neighbor search
 */
public abstract class NumericVectorsFormat {

  /** Sole constructor */
  protected NumericVectorsFormat() {}

  /** Returns a {@link NumericVectorsWriter} to write the vectors to the index. */
  public abstract NumericVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /** Returns a {@link NumericVectorsReader} to read the vectors from the index. */
  public abstract NumericVectorsReader fieldsReader(SegmentReadState state) throws IOException;

  /**
   * EMPTY throws an exception when written. It acts as a sentinel indicating a Codec that does not
   * support vectors.
   */
  public static final NumericVectorsFormat EMPTY =
      new NumericVectorsFormat() {
        @Override
        public NumericVectorsWriter fieldsWriter(SegmentWriteState state) {
          throw new UnsupportedOperationException(
              "Attempt to write EMPTY VectorValues: maybe you forgot to use codec=Lucene90");
        }

        @Override
        public NumericVectorsReader fieldsReader(SegmentReadState state) {
          return new NumericVectorsReader() {
            @Override
            public void checkIntegrity() {}

            @Override
            public NumericVectors getVectorValues(String field) {
              return NumericVectors.EMPTY;
            }

            @Override
            public void close() throws IOException {}

            @Override
            public long ramBytesUsed() {
              return 0;
            }
          };
        }
      };
}
