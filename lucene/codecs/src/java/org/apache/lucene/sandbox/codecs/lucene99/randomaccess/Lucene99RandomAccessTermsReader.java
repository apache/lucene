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
import static org.apache.lucene.sandbox.codecs.lucene99.randomaccess.Lucene99RandomAccessDictionaryPostingsFormat.TERM_DATA_HEADER_CODEC_NAME_PREFIX;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

final class Lucene99RandomAccessTermsReader extends FieldsProducer {
  private final Lucene99PostingsReader postingsReader;
  private final SegmentReadState segmentReadState;

  private final IndexFilesManager indexFilesManager;

  private final HashMap<String, TermsImpl> perFieldTermDict;

  Lucene99RandomAccessTermsReader(
      Lucene99PostingsReader postingsReader, SegmentReadState segmentReadState) throws IOException {
    this.postingsReader = postingsReader;
    this.segmentReadState = segmentReadState;
    this.indexFilesManager = new IndexFilesManager();
    this.perFieldTermDict = new HashMap<>();
    boolean success = false;
    try {
      int numFields = indexFilesManager.metaInfoIn.readVInt();
      assert numFields > 0;
      for (int i = 0; i < numFields; i++) {
        RandomAccessTermsDict termsDict =
            RandomAccessTermsDict.deserialize(
                new RandomAccessTermsDict.IndexOptionsProvider() {
                  @Override
                  public IndexOptions getIndexOptions(int fieldNumber) {
                    return segmentReadState.fieldInfos.fieldInfo(fieldNumber).getIndexOptions();
                  }

                  @Override
                  public boolean hasPayloads(int fieldNumber) {
                    return segmentReadState.fieldInfos.fieldInfo(fieldNumber).hasPayloads();
                  }
                },
                indexFilesManager.metaInfoIn,
                indexFilesManager.termIndexIn,
                indexFilesManager);
        FieldInfo fieldInfo =
            segmentReadState.fieldInfos.fieldInfo(termsDict.termsStats().fieldNumber());
        String fieldName = fieldInfo.name;
        perFieldTermDict.put(fieldName, new TermsImpl(fieldInfo, termsDict, postingsReader));
        success = true;
      }
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(indexFilesManager, postingsReader);
    } finally {
      // The per-field term dictionary would be invalid once the underlying index files have been
      // closed.
      perFieldTermDict.clear();
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    // Integrity is already checked in indexFilesManager
  }

  @Override
  public Iterator<String> iterator() {
    return perFieldTermDict.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    return perFieldTermDict.get(field);
  }

  @Override
  public int size() {
    return perFieldTermDict.size();
  }

  class IndexFilesManager implements RandomAccessTermsDict.TermDataInputProvider, Closeable {
    private final IndexInput metaInfoIn;

    private final IndexInput termIndexIn;

    private final HashMap<TermType, RandomAccessTermsDict.TermDataInput> termDataInputPerType;

    public IndexFilesManager() throws IOException {
      metaInfoIn = initMetaInfoInput();
      termIndexIn = initTermIndexInput();
      termDataInputPerType = new HashMap<>();
    }

    private IndexInput initMetaInfoInput() throws IOException {
      final IndexInput tmp;
      tmp = openAndChecksumIndexInputSafe(TERM_DICT_META_INFO_EXTENSION, false);

      checkHeader(tmp, TERM_DICT_META_HEADER_CODEC_NAME);
      postingsReader.init(tmp, segmentReadState);
      postingsReader.checkIntegrity();
      return tmp;
    }

    private IndexInput initTermIndexInput() throws IOException {
      final IndexInput tmp = openAndChecksumIndexInputSafe(TERM_INDEX_EXTENSION, true);
      checkHeader(tmp, TERM_INDEX_HEADER_CODEC_NAME);
      return tmp;
    }

    private RandomAccessTermsDict.TermDataInput openTermDataInput(TermType termType)
        throws IOException {
      final IndexInput metaTmp;
      final IndexInput dataTmp;
      metaTmp =
          openAndChecksumIndexInputSafe(TERM_DATA_META_EXTENSION_PREFIX + termType.getId(), true);
      checkHeader(metaTmp, TERM_DATA_META_HEADER_CODEC_NAME_PREFIX + termType.getId());

      dataTmp = openAndChecksumIndexInputSafe(TERM_DATA_EXTENSION_PREFIX + termType.getId(), true);
      checkHeader(dataTmp, TERM_DATA_HEADER_CODEC_NAME_PREFIX + termType.getId());

      return new RandomAccessTermsDict.TermDataInput(metaTmp, dataTmp);
    }

    /**
     * Open an IndexInput for a segment local name. The IndexInput will be closed if there was any
     * error happened during open and verification.
     */
    private IndexInput openAndChecksumIndexInputSafe(
        String segmentLocalName, boolean needRandomAccess) throws IOException {
      String name =
          IndexFileNames.segmentFileName(
              segmentReadState.segmentInfo.name, segmentReadState.segmentSuffix, segmentLocalName);

      boolean success = false;
      IndexInput input = null;
      try {
        input =
            segmentReadState.directory.openInput(
                name, needRandomAccess ? IOContext.LOAD : IOContext.READ);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(input);
        }
      }
      CodecUtil.checksumEntireFile(input);
      return input;
    }

    private void checkHeader(IndexInput input, String headerName) throws IOException {
      CodecUtil.checkIndexHeader(
          input,
          headerName,
          Lucene99RandomAccessDictionaryPostingsFormat.VERSION_START,
          Lucene99RandomAccessDictionaryPostingsFormat.VERSION_CURRENT,
          segmentReadState.segmentInfo.getId(),
          segmentReadState.segmentSuffix);
    }

    @Override
    public RandomAccessTermsDict.TermDataInput getTermDataInputForType(TermType termType)
        throws IOException {
      RandomAccessTermsDict.TermDataInput current = termDataInputPerType.get(termType);
      if (current == null) {
        current = openTermDataInput(termType);
        termDataInputPerType.put(termType, current);
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(metaInfoIn, termIndexIn);
      for (var x : termDataInputPerType.values()) {
        IOUtils.close(x.metadataInput(), x.dataInput());
      }
    }
  }
}
