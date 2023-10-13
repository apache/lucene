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
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthCharFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestJapaneseCompletionFilter extends BaseTokenStreamTestCase {
  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.NORMAL);
            return new TokenStreamComponents(
                tokenizer,
                new JapaneseCompletionFilter(tokenizer, JapaneseCompletionFilter.Mode.INDEX));
          }

          @Override
          protected Reader initReader(String fieldName, Reader reader) {
            return new CJKWidthCharFilter(reader);
          }

          @Override
          protected Reader initReaderForNormalization(String fieldName, Reader reader) {
            return new CJKWidthCharFilter(reader);
          }
        };
    queryAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.NORMAL);
            return new TokenStreamComponents(
                tokenizer,
                new JapaneseCompletionFilter(tokenizer, JapaneseCompletionFilter.Mode.QUERY));
          }

          @Override
          protected Reader initReader(String fieldName, Reader reader) {
            return new CJKWidthCharFilter(reader);
          }

          @Override
          protected Reader initReaderForNormalization(String fieldName, Reader reader) {
            return new CJKWidthCharFilter(reader);
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(indexAnalyzer);
    IOUtils.close(queryAnalyzer);
    super.tearDown();
  }

  @Test
  public void testCompletionIndex() throws IOException {
    assertAnalyzesTo(
        indexAnalyzer,
        "東京",
        new String[] {"東京", "toukyou"},
        new int[] {0, 0},
        new int[] {2, 2},
        new int[] {1, 0});

    assertAnalyzesTo(
        indexAnalyzer,
        "東京都",
        new String[] {"東京", "toukyou", "都", "to"},
        new int[] {0, 0, 2, 2},
        new int[] {2, 2, 3, 3},
        new int[] {1, 0, 1, 0});

    assertAnalyzesTo(
        indexAnalyzer,
        "ドラえもん",
        new String[] {"ドラえもん", "doraemon", "doraemonn"},
        new int[] {0, 0, 0},
        new int[] {5, 5, 5},
        new int[] {1, 0, 0});

    assertAnalyzesTo(
        indexAnalyzer,
        "ソースコード",
        new String[] {"ソース", "soーsu", "コード", "koーdo"},
        new int[] {0, 0, 3, 3},
        new int[] {3, 3, 6, 6},
        new int[] {1, 0, 1, 0});

    assertAnalyzesTo(
        indexAnalyzer,
        "反社会的勢力",
        new String[] {"反", "han", "hann", "社会", "syakai", "shakai", "的", "teki", "勢力", "seiryoku"},
        new int[] {0, 0, 0, 1, 1, 1, 3, 3, 4, 4},
        new int[] {1, 1, 1, 3, 3, 3, 4, 4, 6, 6},
        new int[] {1, 0, 0, 1, 0, 0, 1, 0, 1, 0});

    assertAnalyzesTo(
        indexAnalyzer, "々", new String[] {"々"}, new int[] {0}, new int[] {1}, new int[] {1});

    assertAnalyzesTo(
        indexAnalyzer,
        "是々",
        new String[] {"是", "ze", "々"},
        new int[] {0, 0, 1},
        new int[] {1, 1, 2},
        new int[] {1, 0, 1});

    assertAnalyzesTo(
        indexAnalyzer,
        "是々の",
        new String[] {"是", "ze", "々", "の", "no"},
        new int[] {0, 0, 1, 2, 2},
        new int[] {1, 1, 2, 3, 3},
        new int[] {1, 0, 1, 1, 0});
  }

  @Test
  public void testCompletionQuery() throws IOException {
    assertAnalyzesTo(
        queryAnalyzer,
        "東京",
        new String[] {"東京", "toukyou"},
        new int[] {0, 0},
        new int[] {2, 2},
        new int[] {1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "東京都",
        new String[] {"東京", "toukyou", "都", "to"},
        new int[] {0, 0, 2, 2},
        new int[] {2, 2, 3, 3},
        new int[] {1, 0, 1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "ドラえもん",
        new String[] {"ドラえもん", "doraemon", "doraemonn"},
        new int[] {0, 0, 0},
        new int[] {5, 5, 5},
        new int[] {1, 0, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "ソースコード",
        new String[] {"ソースコード", "soーsukoーdo"},
        new int[] {0, 0},
        new int[] {6, 6},
        new int[] {1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "反社会的勢力",
        new String[] {"反", "han", "hann", "社会", "syakai", "shakai", "的", "teki", "勢力", "seiryoku"},
        new int[] {0, 0, 0, 1, 1, 1, 3, 3, 4, 4},
        new int[] {1, 1, 1, 3, 3, 3, 4, 4, 6, 6},
        new int[] {1, 0, 0, 1, 0, 0, 1, 0, 1, 0});

    assertAnalyzesTo(
        queryAnalyzer, "々", new String[] {"々"}, new int[] {0}, new int[] {1}, new int[] {1});

    assertAnalyzesTo(
        queryAnalyzer,
        "是々",
        new String[] {"是", "ze", "々"},
        new int[] {0, 0, 1},
        new int[] {1, 1, 2},
        new int[] {1, 0, 1});

    assertAnalyzesTo(
        indexAnalyzer,
        "是々の",
        new String[] {"是", "ze", "々", "の", "no"},
        new int[] {0, 0, 1, 2, 2},
        new int[] {1, 1, 2, 3, 3},
        new int[] {1, 0, 1, 1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "東京ｔ",
        new String[] {"東京t", "toukyout"},
        new int[] {0, 0},
        new int[] {3, 3},
        new int[] {1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "サッｋ",
        new String[] {"サッk", "sakk"},
        new int[] {0, 0},
        new int[] {3, 3},
        new int[] {1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "反ｓｙ",
        new String[] {"反sy", "hansy", "hannsy"},
        new int[] {0, 0, 0},
        new int[] {3, 3, 3},
        new int[] {1, 0, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "さーきゅｒ",
        new String[] {"さーきゅr", "saーkyur"},
        new int[] {0, 0},
        new int[] {5, 5},
        new int[] {1, 0});

    assertAnalyzesTo(
        queryAnalyzer,
        "是々ｈ",
        new String[] {"是", "ze", "々h"},
        new int[] {0, 0, 1},
        new int[] {1, 1, 3},
        new int[] {1, 0, 1});
  }

  public void testEnglish() throws IOException {
    assertAnalyzesTo(indexAnalyzer, "this atest", new String[] {"this", "atest"});
    assertAnalyzesTo(queryAnalyzer, "this atest", new String[] {"this", "atest"});
  }

  public void testRandomStrings() throws IOException {
    checkRandomData(random(), indexAnalyzer, atLeast(200));
    checkRandomData(random(), queryAnalyzer, atLeast(200));
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new JapaneseCompletionFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
