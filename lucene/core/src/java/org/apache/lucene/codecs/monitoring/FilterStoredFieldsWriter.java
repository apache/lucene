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
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;

/** StoredFieldsWriter implementation that delegates calls to another StoredFieldsWriter. */
public abstract class FilterStoredFieldsWriter extends StoredFieldsWriter {
  protected final StoredFieldsWriter in;

  public FilterStoredFieldsWriter(StoredFieldsWriter in) {
    this.in = in;
  }

  @Override
  public void startDocument() throws IOException {
    in.startDocument();
  }

  @Override
  public void writeField(FieldInfo fieldInfo, IndexableField indexableField) throws IOException {
    in.writeField(fieldInfo, indexableField);
  }

  @Override
  public void finish(int i) throws IOException {
    in.finish(i);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void finishDocument() throws IOException {
    in.finishDocument();
  }
}
