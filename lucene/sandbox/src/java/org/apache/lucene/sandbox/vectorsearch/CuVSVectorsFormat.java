package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;

import com.nvidia.cuvs.CuVSResources;

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
      e.printStackTrace();
    }
  }

  public CuVSVectorsFormat(int cuvsWriterThreads, int intGraphDegree, int graphDegree, MergeStrategy mergeStrategy) {
    super("CuVSVectorsFormat");
    this.mergeStrategy = mergeStrategy;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
    try {
      resources = new CuVSResources();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @Override
  public CuVSVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new CuVSVectorsWriter(state, cuvsWriterThreads, intGraphDegree, graphDegree, mergeStrategy, resources);
  }

  @Override
  public CuVSVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    try {
      return new CuVSVectorsReader(state, resources);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return maxDimensions;
  }

}
