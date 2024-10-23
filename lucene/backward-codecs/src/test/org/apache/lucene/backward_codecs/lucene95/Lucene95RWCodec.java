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
package org.apache.lucene.backward_codecs.lucene95;

import org.apache.lucene.backward_codecs.lucene90.Lucene90RWPostingsFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90RWSegmentInfoFormat;
import org.apache.lucene.backward_codecs.lucene94.Lucene94HnswVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

/** Implements the Lucene 9.5 index format for backwards compat testing */
public class Lucene95RWCodec extends Lucene95Codec {

  private final KnnVectorsFormat defaultKnnVectorsFormat;
  private final KnnVectorsFormat knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return defaultKnnVectorsFormat;
        }
      };
  private final PostingsFormat postingsFormat = new Lucene90RWPostingsFormat();
  private final SegmentInfoFormat segmentInfosFormat = new Lucene90RWSegmentInfoFormat();

  /** Instantiates a new codec. */
  public Lucene95RWCodec() {
    defaultKnnVectorsFormat =
        new Lucene95RWHnswVectorsFormat(
            Lucene94HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene94HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    return knnVectorsFormat;
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfosFormat;
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    return postingsFormat;
  }
}
