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

import static org.apache.lucene.sandbox.codecs.lucene99.randomaccess.Lucene99RandomAccessDictionaryPostingsFormat.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.RandomAccessTermsDictWriter.TermDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

final class Lucene99RandomAccessTermsWriter extends FieldsConsumer {

  private final SegmentWriteState segmentWriteState;

  private final Lucene99PostingsWriter postingsWriter;

  private final IndexFilesManager indexFilesManager;

  private boolean closed;

  public Lucene99RandomAccessTermsWriter(
      SegmentWriteState segmentWriteState, Lucene99PostingsWriter postingsWriter)
      throws IOException {
    this.segmentWriteState = segmentWriteState;
    this.postingsWriter = postingsWriter;
    IndexFilesManager tmpIndexFilesManager = null;
    boolean indexManagerInitSuccess = false;
    try {
      tmpIndexFilesManager = new IndexFilesManager();
      this.indexFilesManager = tmpIndexFilesManager;
      indexManagerInitSuccess = true;
    } finally {
      if (!indexManagerInitSuccess) {
        IOUtils.closeWhileHandlingException(tmpIndexFilesManager, this);
      }
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    HashMap<String, Terms> nonEmptyFields = new HashMap<>();
    for (String field : fields) {
      Terms terms = fields.terms(field);
      if (terms != null) {
        nonEmptyFields.put(field, terms);
      }
    }
    boolean success = false;
    try {
      indexFilesManager.writeAllHeaders();
      postingsWriter.init(indexFilesManager.metaInfoOut, segmentWriteState);
      indexFilesManager.metaInfoOut.writeVInt(nonEmptyFields.size());

      FixedBitSet docSeen = new FixedBitSet(segmentWriteState.segmentInfo.maxDoc());
      for (var entry : nonEmptyFields.entrySet()) {
        TermsEnum termsEnum = entry.getValue().iterator();
        FieldInfo fieldInfo = segmentWriteState.fieldInfos.fieldInfo(entry.getKey());
        RandomAccessTermsDictWriter termsDictWriter =
            new RandomAccessTermsDictWriter(
                fieldInfo.number,
                fieldInfo.getIndexOptions(),
                fieldInfo.hasPayloads(),
                indexFilesManager.metaInfoOut,
                indexFilesManager.termIndexOut,
                indexFilesManager);
        postingsWriter.setField(fieldInfo);

        docSeen.clear();
        while (true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }

          IntBlockTermState termState =
              (IntBlockTermState) postingsWriter.writeTerm(term, termsEnum, docSeen, norms);
          // TermState can be null
          if (termState != null) {
            termsDictWriter.add(term, termState);
          }
        }
        termsDictWriter.finish(docSeen.cardinality());
      }
      indexFilesManager.writeAllFooters();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    IOUtils.close(indexFilesManager, postingsWriter);

    closed = true;
  }

  /**
   * Manages the output index files needed. It handles adding indexing header on creation and footer
   * upon closing.
   */
  class IndexFilesManager implements RandomAccessTermsDictWriter.TermDataOutputProvider, Closeable {

    private final IndexOutput metaInfoOut;

    private final IndexOutput termIndexOut;

    private final HashMap<TermType, TermDataOutput> termDataOutputPerType;

    private boolean closed;

    private final ArrayList<IndexOutput> openedOutputs;

    public IndexFilesManager() throws IOException {
      // populate the per-TermType term data outputs on-demand.
      termDataOutputPerType = new HashMap<>();
      openedOutputs = new ArrayList<>();
      metaInfoOut = initMetaInfoOutput();
      termIndexOut = initTermIndexOutput();
    }

    private IndexOutput initMetaInfoOutput() throws IOException {
      return getIndexOutputSafe(TERM_DICT_META_INFO_EXTENSION);
    }

    private IndexOutput initTermIndexOutput() throws IOException {
      return getIndexOutputSafe(TERM_INDEX_EXTENSION);
    }

    private TermDataOutput initTermDataOutput(TermType termType) throws IOException {
      final IndexOutput metaTmp;
      final IndexOutput dataTmp;
      metaTmp = getIndexOutputSafe(TERM_DATA_META_EXTENSION_PREFIX + termType.getId());
      writeHeader(metaTmp, TERM_DATA_META_HEADER_CODEC_NAME_PREFIX + termType.getId());

      dataTmp = getIndexOutputSafe(TERM_DATA_EXTENSION_PREFIX + termType.getId());
      writeHeader(dataTmp, TERM_DATA_HEADER_CODEC_NAME_PREFIX + termType.getId());

      return new TermDataOutput(metaTmp, dataTmp);
    }

    /**
     * Get an IndexOutput for a segment local name. The output will be closed if there was any error
     * happened during creation.
     */
    private IndexOutput getIndexOutputSafe(String segmentLocalName) throws IOException {
      String name =
          IndexFileNames.segmentFileName(
              segmentWriteState.segmentInfo.name,
              segmentWriteState.segmentSuffix,
              segmentLocalName);

      boolean success = false;
      IndexOutput output = null;
      try {
        output = segmentWriteState.directory.createOutput(name, segmentWriteState.context);
        openedOutputs.add(output);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(output);
        }
      }
      return output;
    }

    private void writeHeader(IndexOutput output, String headerName) throws IOException {
      CodecUtil.writeIndexHeader(
          output,
          headerName,
          Lucene99RandomAccessDictionaryPostingsFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);
    }

    private void writeAllHeaders() throws IOException {
      writeHeader(metaInfoOut, TERM_DICT_META_HEADER_CODEC_NAME);
      writeHeader(termIndexOut, TERM_INDEX_HEADER_CODEC_NAME);
    }

    private void writeAllFooters() throws IOException {
      for (var x : openedOutputs) {
        CodecUtil.writeFooter(x);
      }
    }

    @Override
    public TermDataOutput getTermDataOutputForType(TermType termType) throws IOException {
      TermDataOutput current = termDataOutputPerType.get(termType);
      if (current == null) {
        current = initTermDataOutput(termType);
        termDataOutputPerType.put(termType, current);
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      if (this.closed) {
        return;
      }
      this.closed = true;
      IOUtils.close(openedOutputs);
    }
  }
}
