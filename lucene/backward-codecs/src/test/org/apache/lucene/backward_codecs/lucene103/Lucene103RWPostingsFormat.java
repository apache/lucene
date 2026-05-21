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
package org.apache.lucene.backward_codecs.lucene103;

import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene103.blocktree.Lucene103BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** Read-write impersonation of {@link Lucene103PostingsFormat}. */
public final class Lucene103RWPostingsFormat extends Lucene103PostingsFormat {

  private final int version;
  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /** Creates {@code Lucene103PostingsFormat} with default settings. */
  public Lucene103RWPostingsFormat() {
    this(
        Lucene103BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        Lucene103BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /**
   * Creates {@code Lucene103PostingsFormat} with custom values for {@code minBlockSize} and {@code
   * maxBlockSize} passed to block terms dictionary.
   *
   * @see
   *     Lucene90BlockTreeTermsWriter#Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)
   */
  public Lucene103RWPostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    this(minTermBlockSize, maxTermBlockSize, VERSION_CURRENT);
  }

  /** Expert constructor that allows setting the version. */
  public Lucene103RWPostingsFormat(int minTermBlockSize, int maxTermBlockSize, int version) {
    super();
    if (version < VERSION_START || version > VERSION_CURRENT) {
      throw new IllegalArgumentException("Version out of range: " + version);
    }
    this.version = version;
    Lucene103BlockTreeTermsWriter.validateSettings(minTermBlockSize, maxTermBlockSize);
    this.minTermBlockSize = minTermBlockSize;
    this.maxTermBlockSize = maxTermBlockSize;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene103PostingsWriter(state, version);
    try {
      return new Lucene103BlockTreeTermsWriter(
          state, postingsWriter, minTermBlockSize, maxTermBlockSize);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, postingsWriter);
      throw t;
    }
  }
}
