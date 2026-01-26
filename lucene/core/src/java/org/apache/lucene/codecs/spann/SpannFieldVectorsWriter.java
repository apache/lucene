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
package org.apache.lucene.codecs.spann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers vectors in memory until flush.
 *
 * <p>Future improvements could include off-heap or disk-backed buffering (e.g. ByteBlockPool) to
 * support larger segments without significant heap pressure.
 */
public class SpannFieldVectorsWriter extends KnnFieldVectorsWriter<float[]> {
  private final FieldInfo fieldInfo;
  private final List<float[]> vectors = new ArrayList<>();
  private final List<Integer> docIds = new ArrayList<>();

  private long ramBytesUsed = 0;

  public SpannFieldVectorsWriter(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
  }

  @Override
  public void addValue(int docID, float[] vectorValue) throws IOException {
    vectors.add(vectorValue.clone());
    docIds.add(docID);
    ramBytesUsed += RamUsageEstimator.sizeOf(vectorValue) + Integer.BYTES;
  }

  @Override
  public float[] copyValue(float[] vectorValue) {
    return vectorValue.clone();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed
        + RamUsageEstimator.shallowSizeOf(vectors)
        + RamUsageEstimator.shallowSizeOf(docIds);
  }

  public List<float[]> getVectors() {
    return vectors;
  }

  public List<Integer> getDocIds() {
    return docIds;
  }

  public FieldInfo getFieldInfo() {
    return fieldInfo;
  }
}
