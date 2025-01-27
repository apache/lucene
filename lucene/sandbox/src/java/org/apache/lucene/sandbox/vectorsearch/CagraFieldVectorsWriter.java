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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;

/** CuVS based fields writer */
public class CagraFieldVectorsWriter extends KnnFieldVectorsWriter<float[]> {

  public final String fieldName;
  public final ConcurrentHashMap<Integer, float[]> vectors =
      new ConcurrentHashMap<Integer, float[]>();
  public int fieldVectorDimension = -1;

  public CagraFieldVectorsWriter(FieldInfo fieldInfo) {
    this.fieldName = fieldInfo.getName();
    this.fieldVectorDimension = fieldInfo.getVectorDimension();
  }

  @Override
  public long ramBytesUsed() {
    return fieldName.getBytes(Charset.forName("UTF-8")).length
        + Integer.BYTES
        + (vectors.size() * fieldVectorDimension * Float.BYTES);
  }

  @Override
  public void addValue(int docID, float[] vectorValue) throws IOException {
    vectors.put(docID, vectorValue);
  }

  @Override
  public float[] copyValue(float[] vectorValue) {
    throw new UnsupportedOperationException();
  }
}
