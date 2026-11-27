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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import java.io.StringReader;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanPartOfSpeechStopFilter;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Ignore;

/**
 * Manual smoke test for Nori dictionary loading under a very small heap.
 *
 * <p>Run with:
 *
 * <pre>
 * ./gradlew :lucene:analysis:nori:test --tests TestKoreanDictionarySmallHeap \
 *   -Dtests.jvm.args=-Xmx32m
 * </pre>
 */
@Ignore("Manual small-heap validation; enable locally when verifying off-heap FST loading")
public class TestKoreanDictionarySmallHeap extends LuceneTestCase {

  public void testLoadDictionaryAndTokenizeOnSmallHeap() throws IOException {
    TokenInfoDictionary dictionary = TokenInfoDictionary.getInstance();
    assertNotNull(dictionary.getFST());

    try (KoreanAnalyzer analyzer =
        new KoreanAnalyzer(
            null,
            KoreanTokenizer.DecompoundMode.NONE,
            KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS,
            false)) {
      assertNotNull(analyzer.tokenStream("field", "한국어 형태소 분석"));
    }
  }
}
