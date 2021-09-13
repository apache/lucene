package org.apache.lucene.analysis.ja.completion;

import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class KatakanaRomanizer {
  private static final String ROMAJI_MAP_FILE = "romaji_map.txt";

  private static KatakanaRomanizer INSTANCE;

  static {
    try (InputStreamReader is = new InputStreamReader(KatakanaRomanizer.class.getResourceAsStream(ROMAJI_MAP_FILE));
         BufferedReader ir = new BufferedReader(is)) {
      Map<CharsRef, List<CharsRef>> romajiMap = new HashMap<>();
      String line;
      while ((line = ir.readLine()) != null) {
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
      int maxPrefixLength = keystrokeSet.stream().mapToInt(CharsRef::length).max().getAsInt();
      CharsRef[][] keystrokes = new CharsRef[maxPrefixLength][];
      for (int len = 0; len < maxPrefixLength; len++) {
        final int l = len;
        keystrokes[l] = keystrokeSet.stream().filter(p -> p.length - 1 == l).toArray(CharsRef[]::new);
      }

      INSTANCE = new KatakanaRomanizer(keystrokes, romajiMap);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final CharsRef[][] keystrokes;
  private final Map<CharsRef, List<CharsRef>> romajiMap;

  static KatakanaRomanizer getInstance() {
    return INSTANCE;
  }

  private KatakanaRomanizer(CharsRef[][] keystrokes, Map<CharsRef, List<CharsRef>> romajiMap) {
    this.keystrokes = keystrokes;
    this.romajiMap = romajiMap;
  }

  List<CharsRef> romanize(CharsRef input) {
    assert StringUtils.isKatakanaOrHWAlphabets(input);

    List<CharsRef> pendingOutputs = new ArrayList<>();
    CharsRefBuilder buffer = new CharsRefBuilder();
    int pos = 0;
    while (pos < input.length) {
      // Greedily looks up the longest matched.
      // e.g.: Consider input="キョウ", then there are two matched keystrokes (romaji mapping rules)
      // "キ" -> "ki" and "キョ" -> "kyo". Only the longest one "キョ" will be selected.
      MatchedKeystroke matched = longestKeystrokeMatch(input, pos);
      if (matched == null) {
        break;
      }
      List<CharsRef> outputs = new ArrayList<>();
      List<CharsRef> candidates = romajiMap.get(keystrokes[matched.keystrokeLen-1][matched.keystrokeIndex]);
      for (CharsRef cref : candidates) {
        if (pendingOutputs.size() == 0) {
          // the first matched keystroke; there's no pending output
          outputs.add(cref);
        } else {
          // Combine the matched keystroke with all previously matched keystrokes.
          // e.g.: Consider we already have two pending outputs "shi" and "si" and the matched keystroke "n" and "nn".
          // To produce all possible keystroke patterns, result outputs should be "shin", "shinn", "sin" and "sinn".
          for (CharsRef pdgOutput : pendingOutputs) {
            buffer.copyChars(pdgOutput);
            buffer.append(cref.chars, cref.offset, cref.length);
            outputs.add(buffer.toCharsRef());
          }
        }
      }
      // update the pending outputs
      pendingOutputs = outputs;
      // proceed to the next input position
      pos += matched.keystrokeLen;
    }


    if (pos < input.length) {
      // add the remnants (that cannot be mapped to any romaji) as suffix
      CharsRefBuilder suffix = new CharsRefBuilder();
      for (int i = pos; i < input.length; i++) {
        suffix.append(input.chars[i]);
      }
      return pendingOutputs.stream().map(output -> {
        CharsRefBuilder builder = new CharsRefBuilder();
        builder.copyChars(output);
        builder.append(suffix.chars(), 0, suffix.length());
        return builder.toCharsRef();
      }).collect(Collectors.toList());
    } else {
      return pendingOutputs;
    }

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
  public static void main(String[] main) {
    KatakanaRomanizer instance = KatakanaRomanizer.getInstance();
    CharsRef input = new CharsRef("リーガル");
    for (CharsRef romaji : instance.romanize(input)) {
      System.out.println(romaji);
    }

  }


}
