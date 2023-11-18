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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

final class TermsImpl extends Terms {
  private final FieldInfo fieldInfo;

  private final RandomAccessTermsDict termsDict;

  public TermsImpl(TermsStats stats, FieldInfo fieldInfo, RandomAccessTermsDict termsDict) {
    this.fieldInfo = fieldInfo;
    this.termsDict = termsDict;
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
    // TODO: implement me
    return null;
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    // TODO: implement me
    return null;
  }
}
