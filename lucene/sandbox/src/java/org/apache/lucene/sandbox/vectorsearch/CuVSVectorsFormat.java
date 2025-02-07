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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;

/** CuVS based KnnVectorsFormat for GPU acceleration */
public class CuVSVectorsFormat extends KnnVectorsFormat {

  private static final Logger LOG = Logger.getLogger(CuVSVectorsFormat.class.getName());

  public static final String VECTOR_DATA_CODEC_NAME = "Lucene99CagraVectorsFormatData";
  public static final String VECTOR_DATA_EXTENSION = "cag";
  public static final String META_EXTENSION = "cagmf";
  public static final int VERSION_CURRENT = 0;
  public static final int DEFAULT_WRITER_THREADS = 1;
  public static final int DEFAULT_INTERMEDIATE_GRAPH_DEGREE = 128;
  public static final int DEFAULT_GRAPH_DEGREE = 64;

  static CuVSResources resources = cuVSResourcesOrNull();

  final int maxDimensions = 4096;
  final int cuvsWriterThreads;
  final int intGraphDegree;
  final int graphDegree;
  final MergeStrategy mergeStrategy;

  public CuVSVectorsFormat() {
    this(
        DEFAULT_WRITER_THREADS,
        DEFAULT_INTERMEDIATE_GRAPH_DEGREE,
        DEFAULT_GRAPH_DEGREE,
        MergeStrategy.NON_TRIVIAL_MERGE);
  }

  public CuVSVectorsFormat(
      int cuvsWriterThreads, int intGraphDegree, int graphDegree, MergeStrategy mergeStrategy)
      throws LibraryException {
    super("CuVSVectorsFormat");
    this.mergeStrategy = mergeStrategy;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
  }

  private static CuVSResources cuVSResourcesOrNull() {
    try {
      resources = CuVSResources.create();
      return resources;
    } catch (UnsupportedOperationException uoe) {
      LOG.warning("cuvs is not supported on this platform or java version");
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
    return new CuVSVectorsWriter(
        state, cuvsWriterThreads, intGraphDegree, graphDegree, mergeStrategy, resources);
  }

  @Override
  public CuVSVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    checkSupported();
    try {
      return new CuVSVectorsReader(state, resources);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
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
