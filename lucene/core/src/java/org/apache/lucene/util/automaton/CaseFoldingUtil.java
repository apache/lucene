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
package org.apache.lucene.util.automaton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Generates the case folding set of alternate character mappings based on this spec:
 * https://www.unicode.org/Public/16.0.0/ucd/CaseFolding.txt
 */
class CaseFoldingUtil {

  // FIXME: find an appropriate home for this utility

  /**
   * for updating the case folding mapping from the Unicode case folding file
   *
   * @param args NA
   * @throws IOException when the file can not be found
   */
  public static void main(String[] args) throws IOException {
    verifyPatternCovered();
    generateCaseFoldingMappings();
  }

  private static void verifyPatternCovered() {
    // FIXME: verify that the edge cases found in pattern are covered so that matching
    //  with Pattern is not non-intuitive vs Lucene RegExp ... not sure what to do if they differ...
    generateClass1();
    generateClass2();
    generateClass3();
  }

  private static void generateCaseFoldingMappings() throws IOException {
    // FIXME: use the "class" generator to label each line with a comment ... consider addding in
    //  the "classes" as well below as part of that?
    HexFormat hexFormat = HexFormat.of().withUpperCase();

    // https://www.unicode.org/Public/16.0.0/ucd/CaseFolding.txt
    List<String> caseFoldingData = Files.readAllLines(Path.of("CaseFolding.txt"));

    Map<Integer, List<Integer>> caseFoldedUnicodeMapping = new HashMap<>();
    for (int i = 0; i < caseFoldingData.size(); i++) {
      if (!caseFoldingData.get(i).isEmpty() && !caseFoldingData.get(i).startsWith("#")) {
        String[] line = caseFoldingData.get(i).split(";");

        String type = line[1].strip();
        if (type.equals("C") || type.equals("S") || type.equals("T")) {
          int codePoint = Integer.decode("0x" + line[0].strip());
          int alternate = Integer.decode("0x" + line[2].strip());

          caseFoldedUnicodeMapping
              .computeIfAbsent(codePoint, k -> new ArrayList<>())
              .add(alternate);
          caseFoldedUnicodeMapping
              .computeIfAbsent(alternate, k -> new ArrayList<>())
              .add(codePoint);
        }
      }
    }

    Map<Integer, Set<Integer>> compressedCaseFoldedUnicodeMapping = new HashMap<>();
    for (Map.Entry<Integer, List<Integer>> entry : caseFoldedUnicodeMapping.entrySet()) {
      int codepoint = entry.getKey();

      Queue<Integer> altsToExplore = new LinkedList<>(entry.getValue());

      Integer nextAltCodePoint;
      Set<Integer> compressedAlternates = new HashSet<>();
      while (true) {
        nextAltCodePoint = altsToExplore.poll();

        if (nextAltCodePoint != null) {

          List<Integer> spiderAlts = caseFoldedUnicodeMapping.get(nextAltCodePoint);
          if (compressedAlternates.addAll(spiderAlts)) {
            altsToExplore.addAll(spiderAlts);
          }
        } else {
          break;
        }
      }

      compressedAlternates.remove(codepoint);
      compressedCaseFoldedUnicodeMapping.put(codepoint, compressedAlternates);
    }

    for (Map.Entry<Integer, Set<Integer>> entry : compressedCaseFoldedUnicodeMapping.entrySet()) {
      int codePoint = entry.getKey();
      Set<Integer> alternates = entry.getValue();
      String alternativesString =
          String.join(
              ", ", alternates.stream().map(i -> "0x" + hexFormat.toHexDigits(i, 5)).toList());
      //      System.out.println("0x" + hexFormat.toHexDigits(codePoint, 5) + "; " +
      // alternativesString);
      Files.writeString(
          Path.of("casefolding.txt"),
          "0x" + hexFormat.toHexDigits(codePoint, 5) + "; " + alternativesString + "\n",
          StandardOpenOption.APPEND);
    }
  }

  private static void generateClass1() {
    // the set of alternate matching case-insensitive characters that match with other characters
    // that do not
    // directly go from toLowerCase or toUpperCase
    Map<Integer, List<Integer>> alts = new HashMap<>();
    for (int codePoint = 0; codePoint < Character.MAX_CODE_POINT + 1; codePoint++) {
      int nextCodePoint;
      boolean isLower = Character.isLowerCase(codePoint);
      if (!isLower) {
        boolean isUpper = Character.isUpperCase(codePoint);
        if (!isUpper) {
          continue;
        } else {
          nextCodePoint = Character.toLowerCase(codePoint);
        }
      } else {
        nextCodePoint = Character.toUpperCase(codePoint);
      }

      List<Integer> altArray = new ArrayList<>();
      altArray.add(codePoint);
      altArray.add(nextCodePoint);

      while (codePoint != nextCodePoint) {
        if (Character.isLowerCase(nextCodePoint)) {
          nextCodePoint = Character.toUpperCase(nextCodePoint);
        } else {
          nextCodePoint = Character.toLowerCase(nextCodePoint);
        }
        if (altArray.contains(nextCodePoint)) {
          break;
        } else {
          altArray.add(nextCodePoint);
        }
      }

      if (altArray.size() > 2) {
        alts.put(codePoint, altArray);
      }
    }

    // some combinations spider so we compress/combine them further
    for (int codePoint : new ArrayList<>(alts.keySet())) {
      if (alts.containsKey(codePoint)) {
        List<Integer> altsArray = alts.get(codePoint);
        for (int alt : new ArrayList<>(altsArray)) {
          if (alt == codePoint) {
            continue;
          }
          // for each alternative if we were to make it the key did we discover it earlier
          // if so we need to combine it's spidered alternatives with the original code point and
          // get rid of
          // this entry
          for (Map.Entry<Integer, List<Integer>> entry : new HashSet<>(alts.entrySet())) {
            if (entry.getKey() != codePoint && alts.containsKey(entry.getKey())) {
              if (entry.getValue().contains(alt)) {
                int hiddenCodePoint = entry.getKey();
                alts.get(codePoint).add(hiddenCodePoint);
                alts.remove(hiddenCodePoint);
              }
            }
          }
        }
      }
    }

    Map<String, List<String>> output = new HashMap<>();

    HexFormat hexFormat = HexFormat.of().withUpperCase();
    for (int codePoint : alts.keySet().stream().sorted().toList()) {
      List<Integer> allItems = new ArrayList<>(alts.get(codePoint));

      if (allItems.size() > 2) {
        for (int i = 0; i < allItems.size(); i++) {
          List<Integer> combined = new ArrayList<>();
          combined.addAll(allItems.subList(0, i));
          combined.addAll(allItems.subList(i + 1, allItems.size()));

          List<String> combinedList =
              combined.stream().map(item -> "0x" + hexFormat.toHexDigits(item, 4)).toList();
          //          String combinedString = String.join(", ", combinedList);

          //          System.out.println(
          //              "entry("
          //                  + "0x"
          //                  + hexFormat.toHexDigits(allItems.get(i), 4)
          //                  + ", new int[] {"
          //                  + combinedString
          //                  + "}),");
          output.put("0x" + hexFormat.toHexDigits(allItems.get(i), 4), combinedList);
        }
      }
    }
  }

  private static void generateClass2() {
    // the set of alternate matching case-insensitive characters that match with
    // both an uppercase and a lowercase representation that is different from itself
    Map<String, List<String>> output = new HashMap<>();

    HexFormat hexFormat = HexFormat.of().withUpperCase();
    for (int codePoint = 0; codePoint < Character.MAX_CODE_POINT + 1; codePoint++) {
      if (!new String(Character.toChars(codePoint))
              .toLowerCase(Locale.ROOT)
              .equals(new String(Character.toChars(codePoint)))
          && !new String(Character.toChars(codePoint))
              .toUpperCase(Locale.ROOT)
              .equals(new String(Character.toChars(codePoint)))) {

        String c = "0x" + hexFormat.toHexDigits(codePoint, 4);

        String l =
            Arrays.toString(
                new String(Character.toChars(codePoint))
                    .toLowerCase(Locale.ROOT)
                    .codePoints()
                    .toArray());
        l = "0x" + hexFormat.toHexDigits(Integer.parseInt(l.substring(1, l.length() - 1)), 4);

        int[] uArray =
            new String(Character.toChars(codePoint))
                .toUpperCase(Locale.ROOT)
                .codePoints()
                .toArray();
        String u;
        if (uArray.length > 1) {
          output.put(c, List.of(l));
          output.put(l, List.of(c));
          //          System.out.println("entry(" + c + ", new int[] {" + l + "}),");
          //          System.out.println("entry(" + l + ", new int[] {" + c + "}),");
        } else {
          u = Arrays.toString(uArray);
          u = "0x" + hexFormat.toHexDigits(Integer.parseInt(u.substring(1, u.length() - 1)), 4);
          output.put(c, List.of(l, u));
          output.put(l, List.of(c, u));
          output.put(u, List.of(c, l));
          //          System.out.println("entry(" + c + ", new int[] {" + l + ", " + u + "}),");
          //          System.out.println("entry(" + l + ", new int[] {" + c + ", " + u + "}),");
          //          System.out.println("entry(" + u + ", new int[] {" + c + ", " + l + "}),");
        }
      }
    }
  }

  private static void generateClass3() {
    // the set of alternate non-matching case-insensitive characters that String.toUppercase
    // supports as in the Unicode spec has an upper or lower case character for, but
    // it transitions the BMP, and therefore we don't match the two characters
    List<String> output = new ArrayList<>();
    HexFormat hexFormat = HexFormat.of().withUpperCase();
    int lastCodePoint = -2;
    int inRun = -1;
    for (int codePoint = 0; codePoint < Character.MAX_CODE_POINT + 1; codePoint++) {
      String someChar = new String(Character.toChars(codePoint));
      String hexCodePoint = "0x" + hexFormat.toHexDigits(lastCodePoint, 4);

      // class 3
      // String's support casing across the BMP transition, whereas Characters do not
      if (!someChar
              .toUpperCase(Locale.ROOT)
              .equals(new String(Character.toChars(Character.toUpperCase(codePoint))))
          || !someChar
              .toLowerCase(Locale.ROOT)
              .equals(new String(Character.toChars(Character.toLowerCase(codePoint))))) {
        // System.out.println("class 3 cp: " + codePoint + " :: " + someChar + " :: " +
        // someChar.toUpperCase(Locale.ROOT) + " :: " + someChar.toLowerCase(Locale.ROOT));
        if (codePoint - 1 == lastCodePoint && inRun <= 0) {
          // start a run
          inRun = 1;
          output.add(hexCodePoint);
          //          System.out.print(hexCodePoint);
        } else if (codePoint - 1 != lastCodePoint && inRun == 1) {
          inRun = 0;
          output.add(hexCodePoint);
          //          System.out.print(", " + hexCodePoint + ", ");
        } else if (codePoint - 1 != lastCodePoint && inRun > 1) {
          inRun = 0;
          output.add(hexCodePoint);
          //          System.out.print("-" + hexCodePoint + ", ");
        } else if (inRun <= 0 && lastCodePoint != -2) {
          output.add(hexCodePoint);
          //          System.out.print(hexCodePoint + ", ");
        } else if (inRun == -1) {
          // skip the first one (we output from the second one onward)
        } else {
          inRun++;
        }

        lastCodePoint = codePoint;
      }

      if (codePoint == Character.MAX_CODE_POINT) {
        if (inRun == 1) {
          output.add(hexCodePoint);
          //          System.out.print(", " + hexCodePoint);
        } else if (inRun > 1) {
          output.add(hexCodePoint);
          //          System.out.print("-" + hexCodePoint);
        } else {
          output.add(hexCodePoint);
          //          System.out.print(hexCodePoint);
        }
      }
    }

    //    System.out.println();
  }
}
