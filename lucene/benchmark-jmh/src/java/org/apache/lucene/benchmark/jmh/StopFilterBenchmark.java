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
package org.apache.lucene.benchmark.jmh;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StopFilterBenchmark {

  private static final List<String> ENGLISH_STOP_WORDS =
      Arrays.asList(
          "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
          "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
          "these", "they", "this", "to", "was", "will", "with");

  private static final String[] CONTENT_WORDS = {
    "lucene", "search", "index", "query", "document", "field", "segment", "merge", "filter",
    "score", "boost", "term", "token", "analyzer", "reader", "writer", "directory", "codec",
    "postings", "collection", "virtual"
  };

  @Param({"english", "technical"})
  String textType;

  private CharArraySetBaseline charArraySet;
  private LongHashSet packedSet;
  private char[][] lookupTokens;
  private int[] lookupLengths;

  @Setup(Level.Trial)
  public void setup() {
    charArraySet = new CharArraySetBaseline(ENGLISH_STOP_WORDS);

    packedSet = new LongHashSet(ENGLISH_STOP_WORDS.size());
    for (String w : ENGLISH_STOP_WORDS) {
      char[] chars = w.toLowerCase(java.util.Locale.ROOT).toCharArray();
      packedSet.add(pack(chars, 0, chars.length));
    }

    Random rng = new Random(42);
    int tokenCount = 2000;
    lookupTokens = new char[tokenCount][];
    lookupLengths = new int[tokenCount];
    double stopWordRatio = textType.equals("english") ? 0.50 : 0.20;

    for (int i = 0; i < tokenCount; i++) {
      if (rng.nextDouble() < stopWordRatio) {
        String sw = ENGLISH_STOP_WORDS.get(rng.nextInt(ENGLISH_STOP_WORDS.size()));
        lookupTokens[i] = sw.toCharArray();
      } else {
        String cw = CONTENT_WORDS[rng.nextInt(CONTENT_WORDS.length)];
        lookupTokens[i] = cw.toCharArray();
      }
      lookupLengths[i] = lookupTokens[i].length;
    }
  }

  @Benchmark
  public void charArraySet(Blackhole bh) {
    for (int i = 0; i < lookupTokens.length; i++) {
      bh.consume(charArraySet.contains(lookupTokens[i], 0, lookupLengths[i]));
    }
  }

  @Benchmark
  public void packedLongHashSet(Blackhole bh) {
    for (int i = 0; i < lookupTokens.length; i++) {
      long packed = pack(lookupTokens[i], 0, lookupLengths[i]);
      bh.consume(packed >= 0 && packedSet.contains(packed));
    }
  }

  static long pack(char[] buf, int off, int len) {
    if (len > 8 || len == 0) return -1;
    long packed = 0;
    for (int i = 0; i < len; i++) {
      char c = buf[off + i];
      if (c > 127) return -1;
      if (c >= 'A' && c <= 'Z') c = (char) (c | 0x20);
      packed = (packed << 8) | c;
    }
    return packed;
  }

  static final class CharArraySetBaseline {
    private final char[][] keys;
    private final int mask;

    CharArraySetBaseline(List<String> words) {
      int size = Integer.highestOneBit(Math.max(words.size() * 2, 1)) << 1;
      mask = size - 1;
      keys = new char[size][];
      for (String w : words) {
        char[] key = w.toLowerCase(java.util.Locale.ROOT).toCharArray();
        int slot = hash(key, 0, key.length) & mask;
        while (keys[slot] != null) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = key;
      }
    }

    boolean contains(char[] text, int off, int len) {
      int slot = hash(text, off, len) & mask;
      while (true) {
        char[] stored = keys[slot];
        if (stored == null) return false;
        if (stored.length == len && equals(text, off, len, stored)) return true;
        slot = (slot + 1) & mask;
      }
    }

    private static int hash(char[] text, int off, int len) {
      int code = 0;
      int stop = off + len;
      for (int i = off; i < stop; ) {
        int codePointAt = Character.codePointAt(text, i, stop);
        code = code * 31 + Character.toLowerCase(codePointAt);
        i += Character.charCount(codePointAt);
      }
      return code;
    }

    private static boolean equals(char[] text1, int off, int len, char[] text2) {
      int limit = off + len;
      for (int i = 0; i < len; ) {
        int codePointAt = Character.codePointAt(text1, off + i, limit);
        if (Character.toLowerCase(codePointAt) != Character.codePointAt(text2, i, text2.length)) {
          return false;
        }
        i += Character.charCount(codePointAt);
      }
      return true;
    }
  }
}
