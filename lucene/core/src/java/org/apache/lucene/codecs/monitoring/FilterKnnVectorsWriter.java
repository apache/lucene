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
package org.apache.lucene.codecs.monitoring;

import java.io.IOException;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Sorter;

/** KnnVectorsWriter implementation that delegates calls to another KnnVectorsWriter. */
public abstract class FilterKnnVectorsWriter extends KnnVectorsWriter {
  protected final KnnVectorsWriter in;

  public FilterKnnVectorsWriter(KnnVectorsWriter in) {
    this.in = in;
  }

  @Override
  public long ramBytesUsed() {
    return in.ramBytesUsed();
  }

  @Override
  public KnnFieldVectorsWriter addField(FieldInfo fieldInfo) throws IOException {
    return in.addField(fieldInfo);
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    in.flush(maxDoc, sortMap);
  }

  @Override
  public void finish() throws IOException {
    in.finish();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
