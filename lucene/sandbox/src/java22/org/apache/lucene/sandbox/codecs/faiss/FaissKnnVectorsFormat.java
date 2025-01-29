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
package org.apache.lucene.sandbox.codecs.faiss;

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public final class FaissKnnVectorsFormat extends KnnVectorsFormat {
  public static final String NAME = FaissKnnVectorsFormat.class.getSimpleName();
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = NAME + "Meta";
  static final String DATA_CODEC_NAME = NAME + "Data";
  static final String META_EXTENSION = "faissm";
  static final String DATA_EXTENSION = "faissd";

  private final String description;
  private final String indexParams;
  private final KnnVectorsFormat rawVectorsFormat;

  public FaissKnnVectorsFormat() {
    this("HNSW32", "efConstruction=200");
  }

  public FaissKnnVectorsFormat(String description, String indexParams) {
    super(NAME);
    this.description = description;
    this.indexParams = indexParams;
    this.rawVectorsFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new FaissKnnVectorsWriter(
        description, indexParams, state, rawVectorsFormat.fieldsWriter(state));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new FaissKnnVectorsReader(state, rawVectorsFormat.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT, "%s(description=%s indexParams=%s)", NAME, description, indexParams);
  }
}
