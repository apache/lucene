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
package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsReader;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/**
 * Similar to {@link Lucene99PostingsFormat} but with a different term dictionary implementation.
 *
 * @lucene.experimental
 */
public final class Lucene99RandomAccessDictionaryPostingsFormat extends PostingsFormat {

  // Increment version to change it
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /** Creates {@code Lucene90RandomAccessDictionaryPostingsFormat} */
  public Lucene99RandomAccessDictionaryPostingsFormat() {
    super("Lucene90RandomAccess");
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene99PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret = new Lucene99RandomAccessTermsWriter();
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene99PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer ret = new Lucene99RandomAccessTermsReader();
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
