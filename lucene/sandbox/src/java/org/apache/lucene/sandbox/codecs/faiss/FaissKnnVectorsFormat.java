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

import static org.apache.lucene.util.hnsw.HnswGraphBuilder.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.DEFAULT_MAX_CONN;

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A Faiss-based format to create and search vector indexes, using {@link FaissLibrary} to interact
 * with the native library.
 *
 * <p>The Faiss index is configured using its flexible <a
 * href="https://github.com/facebookresearch/faiss/wiki/The-index-factory">index factory</a>, which
 * allows creating arbitrary indexes by "describing" them. These indexes can be tuned by <a
 * href="https://github.com/facebookresearch/faiss/wiki/Index-IO,-cloning-and-hyper-parameter-tuning">setting
 * relevant parameters</a>.
 *
 * <p>A separate Faiss index is created per-segment, and uses the following files:
 *
 * <ul>
 *   <li><code>.faissm</code> (metadata file): stores field number, offset and length of actual
 *       Faiss index in data file.
 *   <li><code>.faissd</code> (data file): stores concatenated Faiss indexes for all fields.
 *   <li>All files required by {@link Lucene99FlatVectorsFormat} for storing raw vectors.
 * </ul>
 *
 * <p>Note: Set the {@code $OMP_NUM_THREADS} environment variable to control <a
 * href="https://github.com/facebookresearch/faiss/wiki/Threads-and-asynchronous-calls">internal
 * threading</a>.
 *
 * <p>TODO: There is no guarantee of backwards compatibility!
 *
 * @lucene.experimental
 */
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
  private final FlatVectorsFormat rawVectorsFormat;

  /**
   * Constructs an HNSW-based format using default {@code maxConn}={@value
   * org.apache.lucene.util.hnsw.HnswGraphBuilder#DEFAULT_MAX_CONN} and {@code beamWidth}={@value
   * org.apache.lucene.util.hnsw.HnswGraphBuilder#DEFAULT_BEAM_WIDTH}.
   */
  public FaissKnnVectorsFormat() {
    this(
        String.format(Locale.ROOT, "IDMap,HNSW%d", DEFAULT_MAX_CONN),
        String.format(Locale.ROOT, "efConstruction=%d", DEFAULT_BEAM_WIDTH));
  }

  /**
   * Constructs a format using the specified index factory string and index parameters (see class
   * docs for more information).
   *
   * @param description the index factory string to initialize Faiss indexes.
   * @param indexParams the index params to set on Faiss indexes.
   */
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
