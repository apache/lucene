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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;

/** DocValuesConsumer implementation that delegates calls to another DocValuesConsumer. */
public abstract class FilterDocValuesConsumer extends DocValuesConsumer {
  protected final DocValuesConsumer in;

  public FilterDocValuesConsumer(DocValuesConsumer in) {
    this.in = in;
  }

  @Override
  public void addNumericField(FieldInfo fieldInfo, DocValuesProducer docValuesProducer)
      throws IOException {
    in.addNumericField(fieldInfo, docValuesProducer);
  }

  @Override
  public void addBinaryField(FieldInfo fieldInfo, DocValuesProducer docValuesProducer)
      throws IOException {
    in.addBinaryField(fieldInfo, docValuesProducer);
  }

  @Override
  public void addSortedField(FieldInfo fieldInfo, DocValuesProducer docValuesProducer)
      throws IOException {
    in.addSortedField(fieldInfo, docValuesProducer);
  }

  @Override
  public void addSortedNumericField(FieldInfo fieldInfo, DocValuesProducer docValuesProducer)
      throws IOException {
    in.addSortedNumericField(fieldInfo, docValuesProducer);
  }

  @Override
  public void addSortedSetField(FieldInfo fieldInfo, DocValuesProducer docValuesProducer)
      throws IOException {
    in.addSortedSetField(fieldInfo, docValuesProducer);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
