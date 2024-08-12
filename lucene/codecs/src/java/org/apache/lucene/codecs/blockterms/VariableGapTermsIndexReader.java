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
package org.apache.lucene.codecs.blockterms;

import static org.apache.lucene.util.fst.FST.readMetadata;

import java.io.IOException;
import java.util.HashMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * See {@link VariableGapTermsIndexWriter}
 *
 * @lucene.experimental
 */
public class VariableGapTermsIndexReader extends TermsIndexReaderBase {

  private final PositiveIntOutputs fstOutputs = PositiveIntOutputs.getSingleton();

  final HashMap<String, FST<Long>> fields = new HashMap<>();

  public VariableGapTermsIndexReader(SegmentReadState state) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            VariableGapTermsIndexWriter.TERMS_META_EXTENSION);
    String indexFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION);

    try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaFileName);
        ChecksumIndexInput indexIn = state.directory.openChecksumInput(indexFileName)) {

      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            metaIn,
            VariableGapTermsIndexWriter.META_CODEC_NAME,
            VariableGapTermsIndexWriter.VERSION_START,
            VariableGapTermsIndexWriter.VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);

        CodecUtil.checkIndexHeader(
            indexIn,
            VariableGapTermsIndexWriter.CODEC_NAME,
            VariableGapTermsIndexWriter.VERSION_START,
            VariableGapTermsIndexWriter.VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);

        // Read directory
        for (int field = metaIn.readInt(); field != -1; field = metaIn.readInt()) {
          final long indexStart = metaIn.readVLong();
          final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
          if (indexIn.getFilePointer() != indexStart) {
            throw new CorruptIndexException(
                "Gap in FST, expected position " + indexIn.getFilePointer() + ", got " + indexStart,
                metaIn);
          }
          FST<Long> fst = new FST<>(readMetadata(metaIn, fstOutputs), indexIn);
          FST<Long> previous = fields.put(fieldInfo.name, fst);
          if (previous != null) {
            throw new CorruptIndexException("duplicate field: " + fieldInfo.name, metaIn);
          }
        }
      } catch (Throwable t) {
        priorE = t;
      } finally {
        CodecUtil.checkFooter(metaIn, priorE);
        CodecUtil.checkFooter(indexIn, priorE);
      }
    }
  }

  private static class IndexEnum extends FieldIndexEnum {
    private final BytesRefFSTEnum<Long> fstEnum;
    private BytesRefFSTEnum.InputOutput<Long> current;

    public IndexEnum(FST<Long> fst) {
      fstEnum = new BytesRefFSTEnum<>(fst);
    }

    @Override
    public BytesRef term() {
      if (current == null) {
        return null;
      } else {
        return current.input;
      }
    }

    @Override
    public long seek(BytesRef target) throws IOException {
      // System.out.println("VGR: seek field=" + fieldInfo.name + " target=" + target);
      current = fstEnum.seekFloor(target);
      // System.out.println("  got input=" + current.input + " output=" + current.output);
      return current.output;
    }

    @Override
    public long next() throws IOException {
      // System.out.println("VGR: next field=" + fieldInfo.name);
      current = fstEnum.next();
      if (current == null) {
        // System.out.println("  eof");
        return -1;
      } else {
        return current.output;
      }
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long seek(long ord) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean supportsOrd() {
    return false;
  }

  @Override
  public FieldIndexEnum getFieldEnum(FieldInfo fieldInfo) {
    final FST<Long> fieldData = fields.get(fieldInfo.name);
    if (fieldData == null) {
      return null;
    } else {
      return new IndexEnum(fieldData);
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ")";
  }
}
