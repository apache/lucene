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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import opennlp.tools.util.Span;
import org.apache.lucene.analysis.opennlp.tools.NLPSentenceDetectorOp;
import org.apache.lucene.analysis.opennlp.tools.NLPTokenizerOp;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.SentenceAttribute;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IgnoreRandomChains;

/**
 * Run OpenNLP SentenceDetector and Tokenizer. The index of each sentence is stored in
 * SentenceAttribute.
 */
@IgnoreRandomChains(reason = "LUCENE-10352: add argument providers for this one")
public final class OpenNLPTokenizer extends SegmentingTokenizerBase {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final SentenceAttribute sentenceAtt = addAttribute(SentenceAttribute.class);

  private Span[] termSpans = null;
  private int termNum = 0;
  private int sentenceStart = 0;
  private int sentenceIndex = -1;

  private final NLPTokenizerOp tokenizerOp;

  public OpenNLPTokenizer(
      AttributeFactory factory, NLPSentenceDetectorOp sentenceOp, NLPTokenizerOp tokenizerOp)
      throws IOException {
    super(factory, new OpenNLPSentenceBreakIterator(sentenceOp));
    if (sentenceOp == null || tokenizerOp == null) {
      throw new IllegalArgumentException(
          "OpenNLPTokenizer: both a Sentence Detector and a Tokenizer are required");
    }
    this.tokenizerOp = tokenizerOp;
  }

  @Override
  public void close() throws IOException {
    super.close();
    termSpans = null;
    termNum = sentenceStart = 0;
  }

  @Override
  protected void setNextSentence(int sentenceStart, int sentenceEnd) {
    this.sentenceStart = sentenceStart;
    String sentenceText = new String(buffer, sentenceStart, sentenceEnd - sentenceStart);
    termSpans = tokenizerOp.getTerms(sentenceText);
    termNum = 0;
    sentenceIndex++;
  }

  @Override
  protected boolean incrementWord() {
    if (termSpans == null || termNum == termSpans.length) {
      return false;
    }
    clearAttributes();
    Span term = termSpans[termNum];
    termAtt.copyBuffer(buffer, sentenceStart + term.getStart(), term.length());
    offsetAtt.setOffset(
        correctOffset(offset + sentenceStart + term.getStart()),
        correctOffset(offset + sentenceStart + term.getEnd()));
    sentenceAtt.setSentenceIndex(sentenceIndex);
    ++termNum;
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    termSpans = null;
    termNum = sentenceStart = 0;
    sentenceIndex = -1;
  }
}
