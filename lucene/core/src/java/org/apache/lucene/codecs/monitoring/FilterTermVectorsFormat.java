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
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** TermVectorsFormat implementation that delegates calls to another TermVectorsFormat. */
public abstract class FilterTermVectorsFormat extends TermVectorsFormat {
  protected final TermVectorsFormat in;

  public FilterTermVectorsFormat(TermVectorsFormat in) {
    this.in = in;
  }

  @Override
  public TermVectorsReader vectorsReader(
      Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext)
      throws IOException {
    return in.vectorsReader(directory, segmentInfo, fieldInfos, ioContext);
  }

  @Override
  public TermVectorsWriter vectorsWriter(
      Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
    return in.vectorsWriter(directory, segmentInfo, ioContext);
  }
}
