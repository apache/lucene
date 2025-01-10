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
import com.nvidia.cuvs.LibraryNotFoundException;
import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;

public class CuVSVectorsFormat extends KnnVectorsFormat {

  public static final String VECTOR_DATA_CODEC_NAME = "Lucene99CagraVectorsFormatData";
  public static final String VECTOR_DATA_EXTENSION = "cag";
  public static final String META_EXTENSION = "cagmf";
  public static final int VERSION_CURRENT = 0;
  public final int maxDimensions = 4096;
  public final int cuvsWriterThreads;
  public final int intGraphDegree;
  public final int graphDegree;
  public MergeStrategy mergeStrategy;
  public static CuVSResources resources;

  public CuVSVectorsFormat() {
    super("CuVSVectorsFormat");
    this.cuvsWriterThreads = 1;
    this.intGraphDegree = 128;
    this.graphDegree = 64;
    try {
      resources = new CuVSResources();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public CuVSVectorsFormat(
      int cuvsWriterThreads, int intGraphDegree, int graphDegree, MergeStrategy mergeStrategy)
      throws LibraryNotFoundException {
    super("CuVSVectorsFormat");
    this.mergeStrategy = mergeStrategy;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
    try {
      resources = new CuVSResources();
    } catch (LibraryNotFoundException ex) {
      throw ex;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CuVSVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new CuVSVectorsWriter(
        state, cuvsWriterThreads, intGraphDegree, graphDegree, mergeStrategy, resources);
  }

  @Override
  public CuVSVectorsReader fieldsReader(SegmentReadState state) throws IOException {
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
}
