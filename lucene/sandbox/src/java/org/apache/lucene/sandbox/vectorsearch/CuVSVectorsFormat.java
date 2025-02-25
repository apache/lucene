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
package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.LibraryException;
import java.io.IOException;
import java.util.logging.Logger;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.IndexType;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;

/** CuVS based KnnVectorsFormat for GPU acceleration */
public class CuVSVectorsFormat extends KnnVectorsFormat {

  private static final Logger LOG = Logger.getLogger(CuVSVectorsFormat.class.getName());

  // TODO: fix Lucene version in name, to the final targeted release, if any
  static final String CUVS_META_CODEC_NAME = "Lucene102CuVSVectorsFormatMeta";
  static final String CUVS_META_CODEC_EXT = "vemc"; // ""cagmf";
  static final String CUVS_INDEX_CODEC_NAME = "Lucene102CuVSVectorsFormatIndex";
  static final String CUVS_INDEX_EXT = "vcag";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  public static final int DEFAULT_WRITER_THREADS = 32;
  public static final int DEFAULT_INTERMEDIATE_GRAPH_DEGREE = 128;
  public static final int DEFAULT_GRAPH_DEGREE = 64;
  public static final MergeStrategy DEFAULT_MERGE_STRATEGY = MergeStrategy.NON_TRIVIAL_MERGE;
  public static final IndexType DEFAULT_INDEX_TYPE = IndexType.CAGRA;

  static CuVSResources resources = cuVSResourcesOrNull();

  /** The format for storing, reading, and merging raw vectors on disk. */
  private static final FlatVectorsFormat flatVectorsFormat =
      new Lucene99FlatVectorsFormat(DefaultFlatVectorScorer.INSTANCE);

  final int maxDimensions = 4096;
  final int cuvsWriterThreads;
  final int intGraphDegree;
  final int graphDegree;
  final MergeStrategy mergeStrategy;
  final CuVSVectorsWriter.IndexType indexType; // the index type to build, when writing

  /**
   * Creates a CuVSVectorsFormat, with default values.
   *
   * @throws LibraryException if the native library fails to load
   */
  public CuVSVectorsFormat() {
    this(
        DEFAULT_WRITER_THREADS,
        DEFAULT_INTERMEDIATE_GRAPH_DEGREE,
        DEFAULT_GRAPH_DEGREE,
        DEFAULT_MERGE_STRATEGY,
        DEFAULT_INDEX_TYPE);
  }

  /**
   * Creates a CuVSVectorsFormat, with the given threads, graph degree, etc.
   *
   * @throws LibraryException if the native library fails to load
   */
  public CuVSVectorsFormat(
      int cuvsWriterThreads,
      int intGraphDegree,
      int graphDegree,
      MergeStrategy mergeStrategy,
      IndexType indexType) {
    super("CuVSVectorsFormat");
    this.mergeStrategy = mergeStrategy;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
    this.indexType = indexType;
  }

  private static CuVSResources cuVSResourcesOrNull() {
    try {
      resources = CuVSResources.create();
      return resources;
    } catch (UnsupportedOperationException uoe) {
      LOG.warning("cuvs is not supported on this platform or java version: " + uoe.getMessage());
    } catch (Throwable t) {
      if (t instanceof ExceptionInInitializerError ex) {
        t = ex.getCause();
      }
      LOG.warning("Exception occurred during creation of cuvs resources. " + t);
    }
    return null;
  }

  /** Tells whether the platform supports cuvs. */
  public static boolean supported() {
    return resources != null;
  }

  private static void checkSupported() {
    if (!supported()) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public CuVSVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    checkSupported();
    var flatWriter = flatVectorsFormat.fieldsWriter(state);
    return new CuVSVectorsWriter(
        state,
        cuvsWriterThreads,
        intGraphDegree,
        graphDegree,
        mergeStrategy,
        indexType,
        resources,
        flatWriter);
  }

  @Override
  public CuVSVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    checkSupported();
    var flatReader = flatVectorsFormat.fieldsReader(state);
    return new CuVSVectorsReader(state, resources, flatReader);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return maxDimensions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CuVSVectorsFormat(");
    sb.append("cuvsWriterThreads=").append(cuvsWriterThreads);
    sb.append("intGraphDegree=").append(intGraphDegree);
    sb.append("graphDegree=").append(graphDegree);
    sb.append("mergeStrategy=").append(mergeStrategy);
    sb.append("resources=").append(resources);
    sb.append(")");
    return sb.toString();
  }
}
