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
package org.apache.lucene.backward_codecs.lucene92;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

/** Implements the Lucene 9.2 index format for backwards compat testing */
public class Lucene92RWCodec extends Lucene92Codec {

  private final KnnVectorsFormat defaultKnnVectorsFormat;
  private final KnnVectorsFormat knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return defaultKnnVectorsFormat;
        }
      };

  /** Instantiates a new codec. */
  public Lucene92RWCodec() {
    defaultKnnVectorsFormat =
        new Lucene92RWHnswVectorsFormat(
            Lucene92HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene92HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    return knnVectorsFormat;
  }
}
