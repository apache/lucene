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
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** PointsFormat implementation that delegates calls to another PointsFormat. */
public abstract class FilterPointsFormat extends PointsFormat {
  protected final PointsFormat in;

  public FilterPointsFormat(PointsFormat in) {
    this.in = in;
  }

  @Override
  public PointsWriter fieldsWriter(SegmentWriteState var1) throws IOException {
    return in.fieldsWriter(var1);
  }

  @Override
  public PointsReader fieldsReader(SegmentReadState var1) throws IOException {
    return in.fieldsReader(var1);
  }
}
