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
import org.apache.lucene.codecs.lucene99.Lucene99PostingsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.BytesRefPrimitiveLongFSTEnum;

final class TermsImpl extends Terms {
  private final FieldInfo fieldInfo;

  private final RandomAccessTermsDict termsDict;

  private final Lucene99PostingsReader lucene99PostingsReader;

  public TermsImpl(
      FieldInfo fieldInfo,
      RandomAccessTermsDict termsDict,
      Lucene99PostingsReader lucene99PostingsReader) {
    this.fieldInfo = fieldInfo;
    this.termsDict = termsDict;
    this.lucene99PostingsReader = lucene99PostingsReader;
  }

  @Override
  public long size() throws IOException {
    return termsDict.termsStats().size();
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    return termsDict.termsStats().sumTotalTermFreq();
  }

  @Override
  public long getSumDocFreq() throws IOException {
    return termsDict.termsStats().sumDocFreq();
  }

  @Override
  public int getDocCount() throws IOException {
    return termsDict.termsStats().docCount();
  }

  @Override
  public boolean hasFreqs() {
    return fieldInfo.getIndexOptions().ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal();
  }

  @Override
  public boolean hasOffsets() {
    return fieldInfo.getIndexOptions().ordinal()
        >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal();
  }

  @Override
  public boolean hasPositions() {
    return fieldInfo.getIndexOptions().ordinal()
        >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal();
  }

  @Override
  public boolean hasPayloads() {
    return fieldInfo.hasPayloads();
  }

  @Override
  public BytesRef getMin() throws IOException {
    return termsDict.termsStats().minTerm();
  }

  @Override
  public BytesRef getMax() throws IOException {
    return termsDict.termsStats().maxTerm();
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new RandomAccessTermsEnum();
  }

  // TODO: implement a more efficient version via FST
  //  @Override
  //  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException
  // {
  //    return null;
  //  }

  final class RandomAccessTermsEnum extends TermsEnum {
    private AttributeSource attrs;

    private BytesRef term;

    private boolean isTermStateCurrent;

    private IntBlockTermState termState;

    private final BytesRefPrimitiveLongFSTEnum fstEnum;

    private BytesRefPrimitiveLongFSTEnum.InputOutput fstSeekState;

    // Only set when seekExact(term, state) is called, because that will update
    // the termState but leave the fstSeekState out of sync.
    // We need to re-seek in next() calls to catch up to that term.
    private boolean needReSeekInNext;

    private final TermDataReaderProvider.TermDataReader termDataReader;

    RandomAccessTermsEnum() throws IOException {
      termState = (IntBlockTermState) lucene99PostingsReader.newTermState();
      fstEnum = new BytesRefPrimitiveLongFSTEnum(termsDict.termsIndex().primitiveLongFST());
      termDataReader = termsDict.termDataReaderProvider().newReader();
    }

    void updateTermStateIfNeeded() throws IOException {
      if (!isTermStateCurrent && !needReSeekInNext) {
        TermsIndex.TypeAndOrd typeAndOrd = TermsIndex.decodeLong(fstSeekState.output);
        termState =
            termDataReader.getTermState(
                typeAndOrd.termType(), typeAndOrd.ord(), fieldInfo.getIndexOptions());
        isTermStateCurrent = true;
      }
    }

    @Override
    public AttributeSource attributes() {
      if (attrs == null) {
        attrs = new AttributeSource();
      }
      return attrs;
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
      fstSeekState = fstEnum.seekExact(text);
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      return term != null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      fstSeekState = fstEnum.seekCeil(text);
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      if (term == null) {
        return SeekStatus.END;
      }
      return text.equals(term) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
    }

    @Override
    public void seekExact(BytesRef target, TermState state) throws IOException {
      if (!target.equals(term)) {
        assert state instanceof IntBlockTermState;
        termState.copyFrom(state);
        term = BytesRef.deepCopyOf(target);
        isTermStateCurrent = true;
        needReSeekInNext = true;
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public int docFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.postings(fieldInfo, termState, reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.impacts(fieldInfo, termState, flags);
    }

    @Override
    public TermState termState() throws IOException {
      updateTermStateIfNeeded();
      return termState.clone();
    }

    @Override
    public BytesRef next() throws IOException {
      if (needReSeekInNext) {
        fstSeekState = fstEnum.seekExact(term);
        assert fstSeekState != null;
      }
      fstSeekState = fstEnum.next();
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      return term;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException("By ord lookup not supported.");
    }
  }
}
