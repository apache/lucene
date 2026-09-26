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
package org.apache.lucene.analysis.th;

import java.text.BreakIterator;
import java.util.Locale;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArrayIterator;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;
import org.apache.lucene.util.AttributeFactory;

/**
 * Tokenizer that uses {@link BreakIterator} to tokenize Thai text.
 *
 * <p>Supports an optional user dictionary ({@link CharArraySet}) for custom or domain-specific
 * words. When a user dictionary word matches, it takes precedence over default segmentation
 * boundaries.
 *
 * <p>WARNING: this tokenizer may not be supported by all JREs. It is known to work with Sun/Oracle
 * and Harmony JREs. If your application needs to be fully portable, consider using ICUTokenizer
 * instead, which uses an ICU Thai BreakIterator that will always be available.
 */
public class ThaiTokenizer extends SegmentingTokenizerBase {
  /**
   * True if the JRE supports a working dictionary-based breakiterator for Thai. If this is false,
   * this tokenizer will not work at all!
   */
  public static final boolean DBBI_AVAILABLE;

  private static final BreakIterator proto =
      BreakIterator.getWordInstance(new Locale.Builder().setLanguageTag("th").build());

  static {
    // check that we have a working dictionary-based break iterator for thai
    proto.setText("ภาษาไทย");
    DBBI_AVAILABLE = proto.isBoundary(4);
  }

  /** used for breaking the text into sentences */
  private static final BreakIterator sentenceProto = BreakIterator.getSentenceInstance(Locale.ROOT);

  private final BreakIterator wordBreaker;
  private final CharArrayIterator wrapper = CharArrayIterator.newWordInstance();

  private final CharArraySet userDictionary;
  private final int minDictWordLen;
  private final int maxDictWordLen;

  int sentenceStart;
  int sentenceEnd;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /** Creates a new ThaiTokenizer */
  public ThaiTokenizer() {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, null);
  }

  /**
   * Creates a new ThaiTokenizer with a user dictionary.
   *
   * @param userDictionary custom dictionary of words, or null for default dictionary only
   */
  public ThaiTokenizer(CharArraySet userDictionary) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, userDictionary);
  }

  /** Creates a new ThaiTokenizer, supplying the AttributeFactory */
  public ThaiTokenizer(AttributeFactory factory) {
    this(factory, null);
  }

  /**
   * Creates a new ThaiTokenizer, supplying the AttributeFactory and user dictionary.
   *
   * @param factory AttributeFactory to use
   * @param userDictionary custom dictionary of words, or null for default dictionary only
   */
  public ThaiTokenizer(AttributeFactory factory, CharArraySet userDictionary) {
    super(factory, (BreakIterator) sentenceProto.clone());
    if (!DBBI_AVAILABLE) {
      throw new UnsupportedOperationException(
          "This JRE does not have support for Thai segmentation");
    }
    wordBreaker = (BreakIterator) proto.clone();
    this.userDictionary = userDictionary;
    if (userDictionary != null && !userDictionary.isEmpty()) {
      int minLen = Integer.MAX_VALUE;
      int maxLen = 0;
      for (Object obj : userDictionary) {
        int length = (obj instanceof char[]) ? ((char[]) obj).length : obj.toString().length();
        if (length < minLen) {
          minLen = length;
        }
        if (length > maxLen) {
          maxLen = length;
        }
      }
      this.minDictWordLen = minLen;
      this.maxDictWordLen = maxLen;
    } else {
      this.minDictWordLen = 0;
      this.maxDictWordLen = 0;
    }
  }

  @Override
  protected void setNextSentence(int sentenceStart, int sentenceEnd) {
    this.sentenceStart = sentenceStart;
    this.sentenceEnd = sentenceEnd;
    wrapper.setText(buffer, sentenceStart, sentenceEnd - sentenceStart);
    wordBreaker.setText(wrapper);
  }

  @Override
  protected boolean incrementWord() {
    int start = wordBreaker.current();
    if (start == BreakIterator.DONE) {
      return false; // BreakIterator exhausted
    }

    // find the next set of boundaries, skipping over non-tokens
    int end = wordBreaker.next();
    while (end != BreakIterator.DONE
        && !Character.isLetterOrDigit(
            Character.codePointAt(buffer, sentenceStart + start, sentenceEnd))) {
      start = end;
      end = wordBreaker.next();
    }

    if (end == BreakIterator.DONE) {
      return false; // BreakIterator exhausted
    }

    boolean reanchor = false;
    if (userDictionary != null && !userDictionary.isEmpty()) {
      int remaining = sentenceEnd - (sentenceStart + start);
      int maxLen = Math.min(maxDictWordLen, remaining);
      for (int l = maxLen; l >= minDictWordLen; l--) {
        if (userDictionary.contains(buffer, sentenceStart + start, l)) {
          int customEnd = start + l;
          if (customEnd != end) {
            end = customEnd;
            if (end >= sentenceEnd - sentenceStart) {
              wordBreaker.last();
            } else if (wordBreaker.isBoundary(end)) {
              wordBreaker.following(end - 1);
            } else {
              reanchor = true;
            }
          }
          break;
        }
      }
    }

    clearAttributes();
    termAtt.copyBuffer(buffer, sentenceStart + start, end - start);
    offsetAtt.setOffset(
        correctOffset(offset + sentenceStart + start), correctOffset(offset + sentenceStart + end));

    if (reanchor) {
      sentenceStart += end;
      wrapper.setText(buffer, sentenceStart, sentenceEnd - sentenceStart);
      wordBreaker.setText(wrapper);
    }

    return true;
  }
}
