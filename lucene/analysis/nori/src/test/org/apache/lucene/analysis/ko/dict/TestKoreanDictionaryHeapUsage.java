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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.TestKoreanTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.fst.FST;

public class TestKoreanDictionaryHeapUsage extends BaseTokenStreamTestCase {

  private static final int TOKENIZER_COUNT = 50;
  private static final long MAX_FST_RAM_BYTES = 10_000L;

  public void testSystemDictionaryFstIsOffHeap() throws IOException {
    FST<Long> fst = TokenInfoDictionary.getInstance().getFST().getInternalFST();
    assertFstUsesDirectBuffer(fst);
  }

  public void testUserDictionaryFstIsOffHeap() throws IOException {
    UserDictionary userDictionary = TestKoreanTokenizer.readDict();
    FST<Long> fst = userDictionary.getFST().getInternalFST();
    assertFstUsesDirectBuffer(fst);
  }

  public void testManyTokenizersDoNotDuplicateFstBytes() throws IOException {
    UserDictionary userDictionary = TestKoreanTokenizer.readDict();
    List<KoreanTokenizer> tokenizers = new ArrayList<>(TOKENIZER_COUNT);
    for (int i = 0; i < TOKENIZER_COUNT; i++) {
      tokenizers.add(
          new KoreanTokenizer(
              newAttributeFactory(), userDictionary, KoreanTokenizer.DecompoundMode.NONE, false));
    }
    long tokenizerHeap = RamUsageTester.ramUsed(tokenizers);
    assertTrue(
        "expected small per-tokenizer heap footprint, got " + RamUsageTester.humanSizeOf(tokenizers),
        tokenizerHeap < 5_000_000L);
    assertFstUsesDirectBuffer(TokenInfoDictionary.getInstance().getFST().getInternalFST());
  }

  public void testReducedRootCacheUsesLessHeap() throws Exception {
    FST<Long> fst = TokenInfoDictionary.getInstance().getFST().getInternalFST();
    TokenInfoFST cached = new TokenInfoFST(fst, true);
    TokenInfoFST uncached = new TokenInfoFST(fst, false);
    long cachedHeap = RamUsageTester.ramUsed(cached);
    long uncachedHeap = RamUsageTester.ramUsed(uncached);
    assertTrue(cachedHeap > uncachedHeap);
    assertTrue(cachedHeap - uncachedHeap >= 500_000L);
  }

  public void testSegmentationParityWithFullAnalyzer() throws Exception {
    try (Analyzer analyzer = new KoreanAnalyzer()) {
      assertAnalyzesTo(
          analyzer,
          "한국은 대단한 나라입니다.",
          new String[] {"한국", "대단", "나라", "이"},
          new int[] {0, 4, 8, 10},
          new int[] {2, 6, 10, 13},
          new int[] {1, 2, 3, 1});
    }
  }

  private static void assertFstUsesDirectBuffer(FST<Long> fst) {
    assertTrue(fst.ramBytesUsed() < MAX_FST_RAM_BYTES);
  }
}
