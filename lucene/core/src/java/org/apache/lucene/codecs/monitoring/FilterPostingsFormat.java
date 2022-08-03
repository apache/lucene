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
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** PostingsFormat implementation that delegates calls to another PostingsFormat. */
public abstract class FilterPostingsFormat extends PostingsFormat {
  PostingsFormat in;

  public FilterPostingsFormat(PostingsFormat in) {
    super(in.getName());
    this.in = in;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState var1) throws IOException {
    return in.fieldsConsumer(var1);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState var1) throws IOException {
    return in.fieldsProducer(var1);
  }
}
