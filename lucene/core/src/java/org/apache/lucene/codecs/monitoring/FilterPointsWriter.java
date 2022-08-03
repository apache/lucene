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
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;

/** PointsWriter implementation that delegates calls to another PointsWriter. */
public abstract class FilterPointsWriter extends PointsWriter {
  protected final PointsWriter in;

  public FilterPointsWriter(PointsWriter in) {
    this.in = in;
  }

  @Override
  public void writeField(FieldInfo fieldInfo, PointsReader pointsReader) throws IOException {
    in.writeField(fieldInfo, pointsReader);
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
