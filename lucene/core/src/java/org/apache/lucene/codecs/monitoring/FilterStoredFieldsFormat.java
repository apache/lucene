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
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** StoredFieldsFormat implementation that delegates calls to another StoredFieldsFormat. */
public abstract class FilterStoredFieldsFormat extends StoredFieldsFormat {
  protected final StoredFieldsFormat in;

  public FilterStoredFieldsFormat(StoredFieldsFormat in) {
    this.in = in;
  }

  @Override
  public StoredFieldsReader fieldsReader(
      Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext)
      throws IOException {
    return in.fieldsReader(directory, segmentInfo, fieldInfos, ioContext);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(
      Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
    return in.fieldsWriter(directory, segmentInfo, ioContext);
  }
}
