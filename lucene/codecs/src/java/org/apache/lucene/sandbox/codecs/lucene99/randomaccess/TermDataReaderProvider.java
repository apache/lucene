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
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

/** Factory class for {@link TermDataReader} which supports term lookup */
final class TermDataReaderProvider {
  private final TermDataProviderAndCodec[] termDataProviderAndCodecs;

  /** TermDataReader can be reused by the same thread */
  private final ThreadLocal<TermDataReader> termDataReaderReuse;

  TermDataReaderProvider(TermDataProviderAndCodec[] termDataProviderAndCodecs) {
    this.termDataProviderAndCodecs = termDataProviderAndCodecs;
    termDataReaderReuse = new ThreadLocal<>();
  }

  TermDataReader newReader() throws IOException {
    var existingReader = termDataReaderReuse.get();
    if (existingReader != null) {
      return existingReader;
    }
    var newReader = new TermDataReader();
    termDataReaderReuse.set(newReader);
    return newReader;
  }

  static class Builder {
    final IndexOptions indexOptions;
    final boolean hasPayloads;
    final TermDataProviderAndCodec[] termDataProviderAndCodecs =
        new TermDataProviderAndCodec[TermType.NUM_TOTAL_TYPES];

    Builder(IndexOptions indexOptions, boolean hasPayloads) {
      this.indexOptions = indexOptions;
      this.hasPayloads = hasPayloads;
    }

    void readOne(
        TermType termType, DataInput metaIn, IndexInput termMetadataIn, IndexInput termDataIn)
        throws IOException {
      TermDataProvider termDataProvider =
          TermDataProvider.deserializeOffHeap(metaIn, termMetadataIn, termDataIn);
      TermStateCodec codec = TermStateCodecImpl.getCodec(termType, indexOptions, hasPayloads);
      termDataProviderAndCodecs[termType.getId()] =
          new TermDataProviderAndCodec(termDataProvider, codec);
    }

    TermDataReaderProvider build() {
      return new TermDataReaderProvider(termDataProviderAndCodecs);
    }
  }

  record TermDataProviderAndCodec(TermDataProvider termDataProvider, TermStateCodec codec) {}

  public class TermDataReader {
    private final TermData[] termDataPerType;

    private final byte[][] metaDataBufferPerType;

    private final byte[][] dataBufferPerType;

    TermDataReader() throws IOException {
      termDataPerType = new TermData[termDataProviderAndCodecs.length];
      metaDataBufferPerType = new byte[termDataProviderAndCodecs.length][];
      dataBufferPerType = new byte[termDataProviderAndCodecs.length][];

      for (int i = 0; i < termDataProviderAndCodecs.length; i++) {
        if (termDataProviderAndCodecs[i] == null) {
          continue;
        }
        var codec = termDataProviderAndCodecs[i].codec;
        TermDataProvider termDataProvider = termDataProviderAndCodecs[i].termDataProvider;
        termDataPerType[i] =
            new TermData(
                termDataProvider.metadataProvider().newByteSlice(),
                termDataProvider.dataProvider().newByteSlice());
        metaDataBufferPerType[i] = new byte[codec.getMetadataBytesLength()];
        dataBufferPerType[i] = new byte[codec.getMaximumRecordSizeInBytes()];
      }
    }

    IntBlockTermState getTermState(TermType termType, long ord, IndexOptions indexOptions)
        throws IOException {
      assert termDataProviderAndCodecs[termType.getId()] != null;
      assert termDataPerType[termType.getId()] != null;

      int typeId = termType.getId();
      var codec = termDataProviderAndCodecs[termType.getId()].codec;
      IntBlockTermState termState =
          termDataPerType[typeId].getTermStateWithBuffer(
              codec, ord, metaDataBufferPerType[typeId], dataBufferPerType[typeId]);

      // need to filling some default values for the term state
      // in order to meet the expectations of the postings reader
      if (termType.hasSingletonDoc()) {
        termState.docFreq = 1;
      }
      if (termType.hasSkipData() == false) {
        termState.skipOffset = -1;
      }
      if (termType.hasLastPositionBlockOffset() == false) {
        termState.lastPosBlockOffset = -1;
      }

      /* There is interesting conventions to follow...
       * <pre>
       *     org.apache.lucene.index.CheckIndex$CheckIndexException:
       *     field "id" hasFreqs is false, but TermsEnum.totalTermFreq()=0 (should be 1)
       * </pre>
       */
      // for field that do not have freq enabled, as if each posting only has one occurrence.
      if (indexOptions.ordinal() < IndexOptions.DOCS_AND_FREQS.ordinal()) {
        termState.totalTermFreq = termState.docFreq;
      }

      return termState;
    }
  }
}
