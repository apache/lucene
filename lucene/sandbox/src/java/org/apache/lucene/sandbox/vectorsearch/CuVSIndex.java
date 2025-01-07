package org.apache.lucene.sandbox.vectorsearch;

import java.util.List;
import java.util.Objects;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.CagraIndex;

public class CuVSIndex {
  private final CagraIndex cagraIndex;
  private final BruteForceIndex bruteforceIndex;
  private final List<Integer> mapping;
  private final List<float[]> vectors;
  private final int maxDocs;
  
  private final String fieldName;
  private final String segmentName;

  public CuVSIndex(String segmentName, String fieldName, CagraIndex cagraIndex, List<Integer> mapping, List<float[]> vectors, int maxDocs, BruteForceIndex bruteforceIndex) {
    this.cagraIndex = Objects.requireNonNull(cagraIndex);
    this.bruteforceIndex = Objects.requireNonNull(bruteforceIndex);
    this.mapping = Objects.requireNonNull(mapping);
    this.vectors = Objects.requireNonNull(vectors);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.segmentName = Objects.requireNonNull(segmentName);
    this.maxDocs = Objects.requireNonNull(maxDocs);
  }
  
  public CagraIndex getCagraIndex() {
    return cagraIndex;
  }

  public BruteForceIndex getBruteforceIndex() {
    return bruteforceIndex;
  }

  public List<Integer> getMapping() {
    return mapping;
  }

  public String getFieldName() {
    return fieldName;
  }

  public List<float[]> getVectors() {
    return vectors;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public int getMaxDocs() {
    return maxDocs;
  }
}