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
package org.apache.lucene.backward_codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

public class Lucene99RWPostingsFormat extends Lucene99PostingsFormat {

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /** Creates {@code Lucene99PostingsFormat} with default settings. */
  public Lucene99RWPostingsFormat() {
    this(
        Lucene90BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        Lucene90BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /**
   * Creates {@code Lucene99PostingsFormat} with custom values for {@code minBlockSize} and {@code
   * maxBlockSize} passed to block terms dictionary.
   *
   * @see
   *     Lucene90BlockTreeTermsWriter#Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)
   */
  public Lucene99RWPostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super();
    Lucene90BlockTreeTermsWriter.validateSettings(minTermBlockSize, maxTermBlockSize);
    this.minTermBlockSize = minTermBlockSize;
    this.maxTermBlockSize = maxTermBlockSize;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene99PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret =
          new Lucene90BlockTreeTermsWriter(
              state, postingsWriter, minTermBlockSize, maxTermBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }
}
