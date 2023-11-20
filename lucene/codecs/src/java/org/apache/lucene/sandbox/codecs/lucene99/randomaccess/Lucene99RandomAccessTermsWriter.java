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
    this.indexFilesManager = new IndexFilesManager();
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
    indexFilesManager.metaInfoOut.writeVInt(nonEmptyFields.size());

    FixedBitSet docSeen = new FixedBitSet(segmentWriteState.segmentInfo.maxDoc());
    for (var entry : nonEmptyFields.entrySet()) {
      TermsEnum termsEnum = entry.getValue().iterator();
      FieldInfo fieldInfo = segmentWriteState.fieldInfos.fieldInfo(entry.getKey());
      RandomAccessTermsDictWriter termsDictWriter =
          new RandomAccessTermsDictWriter(
              fieldInfo.number,
              fieldInfo.getIndexOptions(),
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
        termsDictWriter.add(term, termState);
      }
      termsDictWriter.finish(docSeen.cardinality());
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

    public IndexFilesManager() throws IOException {
      metaInfoOut = initMetaInfoOutput();
      termIndexOut = initTermIndexOutput();
      // populate the per-TermType term data outputs on-demand.
      termDataOutputPerType = new HashMap<>();
    }

    private IndexOutput initMetaInfoOutput() throws IOException {
      final IndexOutput tmp;
      tmp = getIndexOutputSafe(TERM_DICT_META_INFO_EXTENSION);
      writeHeader(tmp, TERM_DICT_META_HEADER_CODEC_NAME);
      postingsWriter.init(tmp, segmentWriteState);
      return tmp;
    }

    private IndexOutput initTermIndexOutput() throws IOException {
      final IndexOutput tmp = getIndexOutputSafe(TERM_INDEX_EXTENSION);
      writeHeader(tmp, TERM_INDEX_HEADER_CODEC_NAME);
      return tmp;
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

    @Override
    public TermDataOutput getTermDataOutputForType(TermType termType) throws IOException {
      TermDataOutput current = termDataOutputPerType.get(termType);
      if (current == null) {
        current = initTermDataOutput(termType);
        termDataOutputPerType.put(termType, current);
      }
      return current;
    }

    /**
     * Write footers for all created index files and close them.
     *
     * <p>Assume all index files are valid upto time of calling.
     */
    public void close() throws IOException {
      boolean success = false;
      try {
        CodecUtil.writeFooter(metaInfoOut);
        CodecUtil.writeFooter(termIndexOut);
        for (var termDataOutput : termDataOutputPerType.values()) {
          CodecUtil.writeFooter(termDataOutput.metadataOutput());
          CodecUtil.writeFooter(termDataOutput.dataOutput());
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(metaInfoOut, termIndexOut);
          for (var termDataOutput : termDataOutputPerType.values()) {
            IOUtils.close(termDataOutput.metadataOutput());
            IOUtils.close(termDataOutput.dataOutput());
          }
        } else {
          IOUtils.closeWhileHandlingException(metaInfoOut, termIndexOut);
          for (var termDataOutput : termDataOutputPerType.values()) {
            IOUtils.closeWhileHandlingException(
                termDataOutput.metadataOutput(), termDataOutput.dataOutput());
          }
        }
      }
    }
  }
}
