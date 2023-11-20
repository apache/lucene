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
  static String TERM_DICT_META_HEADER_CODEC_NAME = "RandomAccessTermsDict";
  static String TERM_INDEX_HEADER_CODEC_NAME = "RandomAccessTermsDictIndex";
  static String TERM_DATA_META_HEADER_CODEC_NAME_PREFIX = "RandomAccessTermsDictTermDataMeta";
  static String TERM_DATA_HEADER_CODEC_NAME_PREFIX = "RandomAccessTermsDictTermData";

  static String TERM_DICT_META_INFO_EXTENSION = "tmeta";
  static String TERM_INDEX_EXTENSION = "tidx";
  static String TERM_DATA_META_EXTENSION_PREFIX = "tdm";
  static String TERM_DATA_EXTENSION_PREFIX = "tdd";

  // Increment version to change it
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /** Creates {@code Lucene90RandomAccessDictionaryPostingsFormat} */
  public Lucene99RandomAccessDictionaryPostingsFormat() {
    super("Lucene99RandomAccess");
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    Lucene99PostingsWriter postingsWriter = new Lucene99PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret = new Lucene99RandomAccessTermsWriter(state, postingsWriter);
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
    Lucene99PostingsReader postingsReader = new Lucene99PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer ret = new Lucene99RandomAccessTermsReader(postingsReader, state);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
