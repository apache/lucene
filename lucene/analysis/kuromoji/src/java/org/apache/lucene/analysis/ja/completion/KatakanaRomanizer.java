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
package org.apache.lucene.analysis.ja.completion;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * Converts a Katakana string to <a
 * href="https://en.wikipedia.org/wiki/Romanization_of_Japanese">Romaji</a> using the pre-defined
 * Katakana-Romaji mapping rules. Internally, this repeatedly performs prefix match on the given
 * char sequence to the pre-built keystroke array until it reaches the end of the sequence, or there
 * are no matched keystrokes.
 */
public class KatakanaRomanizer {
  private static final String ROMAJI_MAP_FILE = "romaji_map.txt";

  private static KatakanaRomanizer INSTANCE;

  static {
    // Build romaji-map and keystroke arrays from the pre-defined Katakana-Romaji mapping file.
    try (InputStreamReader is =
            new InputStreamReader(
                KatakanaRomanizer.class.getResourceAsStream(ROMAJI_MAP_FILE),
                Charset.forName("UTF-8"));
        BufferedReader ir = new BufferedReader(is)) {
      Map<CharsRef, List<CharsRef>> romajiMap = new HashMap<>();
      String line;
      while ((line = ir.readLine()) != null) {
        if (line.startsWith("#")) {
          continue;
        }
        String[] cols = line.trim().split(",");
        if (cols.length < 2) {
          continue;
        }
        CharsRef prefix = new CharsRef(cols[0]);
        romajiMap.put(prefix, new ArrayList<>());
        for (int i = 1; i < cols.length; i++) {
          romajiMap.get(prefix).add(new CharsRef(cols[i]));
        }
      }

      Set<CharsRef> keystrokeSet = romajiMap.keySet();
      int maxKeystrokeLength = keystrokeSet.stream().mapToInt(CharsRef::length).max().getAsInt();
      CharsRef[][] keystrokes = new CharsRef[maxKeystrokeLength][];
      for (int len = 0; len < maxKeystrokeLength; len++) {
        final int l = len;
        keystrokes[l] =
            keystrokeSet.stream().filter(k -> k.length - 1 == l).toArray(CharsRef[]::new);
      }
      for (CharsRef[] ks : keystrokes) {
        // keystroke array must be sorted in ascending order for binary search.
        Arrays.sort(ks);
      }

      INSTANCE = new KatakanaRomanizer(keystrokes, romajiMap);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final CharsRef[][] keystrokes;
  private final Map<CharsRef, List<CharsRef>> romajiMap;

  /** Returns the singleton instance of {@code KatakanaRomenizer} */
  public static KatakanaRomanizer getInstance() {
    return INSTANCE;
  }

  private KatakanaRomanizer(CharsRef[][] keystrokes, Map<CharsRef, List<CharsRef>> romajiMap) {
    this.keystrokes = keystrokes;
    this.romajiMap = romajiMap;
  }

  /**
   * Translates a sequence of katakana to romaji. An input can produce multiple outputs because a
   * keystroke can be mapped to multiple romajis.
   */
  public List<CharsRef> romanize(CharsRef input) {
    assert CharSequenceUtils.isKatakanaOrHWAlphabets(input);

    List<CharsRefBuilder> pendingOutputs = new ArrayList<>();
    int pos = 0;
    while (pos < input.length) {
      // Greedily looks up the longest matched keystroke.
      // e.g.: Consider input="キョウ", then there are two matched keystrokes (romaji mapping rules)
      // "キ" -> "ki" and "キョ" -> "kyo". Only the longest one "キョ" will be selected.
      MatchedKeystroke matched = longestKeystrokeMatch(input, pos);
      if (matched == null) {
        break;
      }

      List<CharsRef> candidates =
          romajiMap.get(keystrokes[matched.keystrokeLen - 1][matched.keystrokeIndex]);

      if (pendingOutputs.size() == 0) {
        // There is no pending output.
        // Add the matched keystrokes to pending outputs list.
        for (CharsRef cref : candidates) {
          CharsRefBuilder output = new CharsRefBuilder();
          output.copyChars(cref);
          pendingOutputs.add(output);
        }
      } else if (candidates.size() == 1) {
        // There are one or more pending output(s) and one matched keystroke.
        // Append the matched keystroke to all pending outputs.
        // e.g.: Consider we already have two pending outputs "shi" and "si" and the matched
        // keystroke "ka";
        // then results are "shika" and "sika".
        CharsRef cref = candidates.get(0);
        for (CharsRefBuilder pdgOutput : pendingOutputs) {
          pdgOutput.append(cref.chars, 0, cref.length);
        }
      } else {
        // There are one or more pending output(s) and multiple matched keystrokes.
        // Combine the matched keystrokes to all pending outputs.
        // e.g.: Consider we already have two pending outputs "shi" and "si" and the matched
        // keystroke "n" and "nn".
        // To produce all possible keystroke patterns, result outputs should be "shin", "shinn",
        // "sin" and "sinn".
        List<CharsRefBuilder> outputs = new ArrayList<>();
        for (CharsRef cref : candidates) {
          for (CharsRefBuilder pdgOutput : pendingOutputs) {
            CharsRefBuilder buffer = new CharsRefBuilder();
            buffer.copyChars(pdgOutput.chars(), 0, pdgOutput.length());
            buffer.append(cref.chars, cref.offset, cref.length);
            outputs.add(buffer);
          }
        }
        // update the pending outputs
        pendingOutputs = outputs;
      }

      // proceed to the next input position
      pos += matched.keystrokeLen;
    }

    if (pos < input.length) {
      // add the remnants (that cannot be mapped to any romaji) as suffix
      for (CharsRefBuilder output : pendingOutputs) {
        output.append(input.chars, pos, input.length - pos);
      }
    }
    return pendingOutputs.stream().map(CharsRefBuilder::get).collect(Collectors.toList());
  }

  private MatchedKeystroke longestKeystrokeMatch(CharsRef input, int inputOffset) {
    for (int len = Math.min(input.length - inputOffset, keystrokes.length); len > 0; len--) {
      CharsRef ref = new CharsRef(input.chars, inputOffset, len);
      int index = Arrays.binarySearch(keystrokes[len - 1], ref);
      if (index >= 0) {
        return new MatchedKeystroke(len, index);
      }
    }
    // there's no matched keystroke
    return null;
  }

  private static class MatchedKeystroke {
    final int keystrokeLen;
    final int keystrokeIndex;

    MatchedKeystroke(int keystrokeLen, int keystrokeIndex) {
      this.keystrokeLen = keystrokeLen;
      this.keystrokeIndex = keystrokeIndex;
    }
  }
}
