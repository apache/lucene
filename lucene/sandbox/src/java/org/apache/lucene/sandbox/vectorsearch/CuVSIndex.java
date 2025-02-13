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

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.CagraIndex;
import java.util.List;
import java.util.Objects;

/** This class holds references to the actual CuVS Index (Cagra, Brute force, etc.) */
/*package-private*/ class CuVSIndex {
  private final CagraIndex cagraIndex;
  private final BruteForceIndex bruteforceIndex;
  private final List<Integer> mapping;
  private final List<float[]> vectors;
  private final int maxDocs;

  private final String fieldName;
  private final String segmentName;

  public CuVSIndex(
      String segmentName,
      String fieldName,
      CagraIndex cagraIndex,
      List<Integer> mapping,
      List<float[]> vectors,
      int maxDocs,
      BruteForceIndex bruteforceIndex) {
    this.cagraIndex = Objects.requireNonNull(cagraIndex);
    this.bruteforceIndex = Objects.requireNonNull(bruteforceIndex);
    this.mapping = Objects.requireNonNull(mapping);
    this.vectors = Objects.requireNonNull(vectors);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.segmentName = Objects.requireNonNull(segmentName);
    if (maxDocs < 0) {
      throw new IllegalArgumentException("negative maxDocs:" + maxDocs);
    }
    this.maxDocs = maxDocs;
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
