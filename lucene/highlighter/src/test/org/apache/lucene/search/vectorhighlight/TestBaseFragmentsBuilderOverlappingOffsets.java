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
package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests that BaseFragmentsBuilder handles overlapping token offsets correctly.
 *
 * <p>Analyzers like CJK bigram, ik_max_word, and other multi-granularity tokenizers produce tokens
 * with overlapping offsets. For example, "中华人民共和国" may produce tokens at [0,2), [0,4), [1,3),
 * [2,4), [2,7), [4,6), [4,7). The highlighter must handle these without throwing
 * StringIndexOutOfBoundsException and should highlight the full matched region.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/issues/73072">Elasticsearch #73072</a>
 */
public class TestBaseFragmentsBuilderOverlappingOffsets extends LuceneTestCase {

  private static final Encoder ENCODER = new DefaultEncoder();
  private static final String[] PRE_TAGS = {"<b>"};
  private static final String[] POST_TAGS = {"</b>"};

  private String makeFragment(String sourceText, WeightedFragInfo fragInfo) {
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    Field[] values = new Field[] {new Field("f", sourceText, ft)};
    StringBuilder buffer = new StringBuilder();
    int[] index = {0};
    return sfb.makeFragment(buffer, index, values, fragInfo, PRE_TAGS, POST_TAGS, ENCODER);
  }

  /**
   * Overlapping tokens [0,10) and [8,14): the second token starts before the first one ends. Before
   * the fix this throws StringIndexOutOfBoundsException: Range [10, 8) out of bounds.
   */
  public void testOverlappingTokensHighlightExtendedRegion() {
    String src = "hello world test data";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(0, 10));
    toffsList.add(new Toffs(8, 14));

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 1.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 21, subInfos, 1.0f);
    String result = makeFragment(src, fragInfo);

    // [0,10) highlighted, then [8,14) clipped to [10,14)
    assertEquals("<b>hello worl</b><b>d te</b>st data", result);
  }

  /**
   * Token entirely contained within a previously highlighted region should be skipped without error.
   */
  public void testFullyContainedTokenIsSkipped() {
    String src = "abcdefghij";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(0, 8)); // covers [0,8)
    toffsList.add(new Toffs(2, 5)); // entirely within [0,8)

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 1.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 10, subInfos, 1.0f);
    String result = makeFragment(src, fragInfo);

    assertEquals("<b>abcdefgh</b>ij", result);
  }

  /**
   * CJK max-word segmentation: "中华人民共和国" produces 7 overlapping tokens that collectively
   * cover [0,7). All characters must be highlighted.
   */
  public void testCjkMaxWordOverlappingTokens() {
    String src = "中华人民共和国";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(0, 2)); // 中华
    toffsList.add(new Toffs(0, 4)); // 中华人民
    toffsList.add(new Toffs(1, 3)); // 华人
    toffsList.add(new Toffs(2, 4)); // 人民
    toffsList.add(new Toffs(2, 7)); // 人民共和国
    toffsList.add(new Toffs(4, 6)); // 共和
    toffsList.add(new Toffs(4, 7)); // 共和国

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 7.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 7, subInfos, 7.0f);
    String result = makeFragment(src, fragInfo);

    // Tokens: [0,2) -> "中华", [0,4) clipped to [2,4) -> "人民",
    // [2,7) clipped to [4,7) -> "共和国". Rest fully contained, skipped.
    assertEquals("<b>中华</b><b>人民</b><b>共和国</b>", result);
  }

  /** CJK overlapping tokens with surrounding non-highlighted context. */
  public void testCjkMaxWordWithContext() {
    String src = "我爱中华人民共和国万岁";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(2, 4)); // 中华
    toffsList.add(new Toffs(2, 6)); // 中华人民
    toffsList.add(new Toffs(3, 5)); // 华人
    toffsList.add(new Toffs(4, 6)); // 人民
    toffsList.add(new Toffs(4, 9)); // 人民共和国
    toffsList.add(new Toffs(6, 8)); // 共和
    toffsList.add(new Toffs(6, 9)); // 共和国

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 7.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 11, subInfos, 7.0f);
    String result = makeFragment(src, fragInfo);

    assertEquals("我爱<b>中华</b><b>人民</b><b>共和国</b>万岁", result);
  }

  /** Token whose endOffset exceeds source length should be skipped. */
  public void testTokenExceedingSourceLength() {
    String src = "short";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(0, 3)); // valid
    toffsList.add(new Toffs(3, 99)); // endOffset > src.length()

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 1.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 5, subInfos, 1.0f);
    String result = makeFragment(src, fragInfo);

    assertEquals("<b>sho</b>rt", result);
  }

  /** Multiple SubInfos with overlapping offsets across different query terms. */
  public void testOverlappingAcrossSubInfos() {
    String src = "the quick brown fox";

    List<Toffs> toffs1 = new ArrayList<>();
    toffs1.add(new Toffs(4, 9)); // "quick"
    SubInfo sub1 = new SubInfo("quick", toffs1, 0, 1.0f);

    List<Toffs> toffs2 = new ArrayList<>();
    toffs2.add(new Toffs(7, 14)); // "ck brow" - overlaps with "quick"
    SubInfo sub2 = new SubInfo("overlap", toffs2, 1, 1.0f);

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(sub1);
    subInfos.add(sub2);

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 19, subInfos, 2.0f);
    String result = makeFragment(src, fragInfo);

    // "quick" [4,9) highlighted, then [7,14) clipped to [9,14) = " brow"
    assertEquals("the <b>quick</b><b> brow</b>n fox", result);
  }

  /** Inverted offset (endOffset < startOffset) should be skipped without error. */
  public void testInvertedOffsetsSkipped() {
    String src = "hello world";

    List<Toffs> toffsList = new ArrayList<>();
    toffsList.add(new Toffs(5, 3)); // inverted: end < start
    toffsList.add(new Toffs(0, 5)); // valid

    List<SubInfo> subInfos = new ArrayList<>();
    subInfos.add(new SubInfo("term", toffsList, 0, 1.0f));

    WeightedFragInfo fragInfo = new WeightedFragInfo(0, 11, subInfos, 1.0f);
    String result = makeFragment(src, fragInfo);

    // Inverted offset skipped, valid token highlighted
    assertEquals("<b>hello</b> world", result);
  }
}
