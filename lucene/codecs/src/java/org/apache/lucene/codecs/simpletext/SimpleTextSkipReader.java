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
package org.apache.lucene.codecs.simpletext;

import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.FREQ;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACT;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.IMPACTS_END;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.NORM;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_DOC;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_DOC_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_LIST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * This class reads skip lists with multiple levels.
 *
 * <p>See {@link SimpleTextFieldsWriter} for the information about the encoding of the multi level
 * skip lists.
 *
 * @lucene.experimental
 */
public class SimpleTextSkipReader extends MultiLevelSkipListReader {

  private final CharsRefBuilder scratchUTF16 = new CharsRefBuilder();
  private final Impacts impacts;
  private final List<List<Impact>> perLevelImpacts;
  private long nextSkipDocFP = -1;
  private final int nextSkipDoc[];

  public SimpleTextSkipReader(IndexInput skipStream, long docStartFP, int docCount)
      throws IOException {
    super(
        skipStream,
        SimpleTextSkipWriter.maxSkipLevels,
        SimpleTextSkipWriter.BLOCK_SIZE,
        SimpleTextSkipWriter.skipMultiplier);
    init(skipStream, docStartFP, docCount);
    nextSkipDoc = new int[SimpleTextSkipWriter.maxSkipLevels];
    Arrays.fill(nextSkipDoc, 0);
    perLevelImpacts = new ArrayList<>(maxNumberOfSkipLevels);
    for (int level = 0; level < maxNumberOfSkipLevels; level++) {
      perLevelImpacts.add(level, new ArrayList<>());
    }
    impacts =
        new Impacts() {
          @Override
          public int numLevels() {
            return maxNumberOfSkipLevels;
          }

          @Override
          public int getDocIdUpTo(int level) {
            return skipDoc[level];
          }

          @Override
          public List<Impact> getImpacts(int level) {
            return perLevelImpacts.get(level);
          }
        };
  }

  @Override
  public int skipTo(int target) throws IOException {
    int result = super.skipTo(target);
    for (int level = 0; level < SimpleTextSkipWriter.maxSkipLevels; level++) {
      if (super.skipDoc[level] != DocIdSetIterator.NO_MORE_DOCS) {
        // because the MultiLevelSkipListReader stores doc id delta,but simple text codec stores doc
        // id,so we override the super.skipDoc
        super.skipDoc[level] = this.nextSkipDoc[level];
      }
    }
    return result;
  }

  @Override
  protected int readSkipData(int level, IndexInput skipStream) throws IOException {
    perLevelImpacts.get(level).clear();
    int skipDoc = DocIdSetIterator.NO_MORE_DOCS;
    ChecksumIndexInput input = new BufferedChecksumIndexInput(skipStream);
    BytesRefBuilder scratch = new BytesRefBuilder();
    int freq = 1;
    while (true) {
      SimpleTextUtil.readLine(skipStream, scratch);
      if (scratch.get().equals(SimpleTextFieldsWriter.END)) {
        SimpleTextUtil.checkFooter(input);
        break;
      } else if (scratch.get().equals(IMPACTS_END)
          || scratch.get().equals(SimpleTextFieldsWriter.TERM)
          || scratch.get().equals(SimpleTextFieldsWriter.FIELD)) {
        break;
      } else if (StringHelper.startsWith(scratch.get(), SKIP_DOC)) {
        scratchUTF16.copyUTF8Bytes(
            scratch.bytes(), SKIP_DOC.length, scratch.length() - SKIP_DOC.length);
        skipDoc = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
        nextSkipDoc[level] = skipDoc;
      } else if (StringHelper.startsWith(scratch.get(), SKIP_DOC_FP)) {
        scratchUTF16.copyUTF8Bytes(
            scratch.bytes(), SKIP_DOC_FP.length, scratch.length() - SKIP_DOC_FP.length);
        nextSkipDocFP = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
      } else if (StringHelper.startsWith(scratch.get(), IMPACTS)
          || StringHelper.startsWith(scratch.get(), IMPACT)) {
        // continue;
      } else if (StringHelper.startsWith(scratch.get(), FREQ)) {
        scratchUTF16.copyUTF8Bytes(scratch.bytes(), FREQ.length, scratch.length() - FREQ.length);
        freq = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
      } else if (StringHelper.startsWith(scratch.get(), NORM)) {
        scratchUTF16.copyUTF8Bytes(scratch.bytes(), NORM.length, scratch.length() - NORM.length);
        int norm = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
        Impact impact = new Impact(freq, norm);
        perLevelImpacts.get(level).add(impact);
      }
    }
    return skipDoc;
  }

  public void init(IndexInput skipStream, long docStartFP, int docCount) throws IOException {
    long skipPointer = -1;
    skipStream.seek(docStartFP);
    ChecksumIndexInput input = new BufferedChecksumIndexInput(skipStream);
    BytesRefBuilder scratch = new BytesRefBuilder();
    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (scratch.get().equals(SimpleTextFieldsWriter.END)) {
        SimpleTextUtil.checkFooter(input);
        break;
      } else if (StringHelper.startsWith(scratch.get(), SimpleTextFieldsWriter.TERM)
          || StringHelper.startsWith(scratch.get(), SimpleTextFieldsWriter.FIELD)) {
        break;
      } else if (StringHelper.startsWith(scratch.get(), SKIP_LIST)) {
        skipPointer = input.getFilePointer();
        break;
      }
    }
    super.init(skipPointer, docCount);
  }

  Impacts getImpacts() {
    return impacts;
  }

  long getNextSkipDocFP() {
    return nextSkipDocFP;
  }

  int getNextSkipDoc() {
    return skipDoc[0];
  }
}
