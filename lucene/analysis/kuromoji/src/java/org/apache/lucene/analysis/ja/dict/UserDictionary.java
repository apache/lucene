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
package org.apache.lucene.analysis.ja.dict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.morph.Dictionary;
import org.apache.lucene.analysis.util.CSVUtil;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/** Class for building a User Dictionary. This class allows for custom segmentation of phrases. */
public final class UserDictionary implements Dictionary<UserMorphData> {

  public static final String INTERNAL_SEPARATOR = "\u0000";

  private static final Pattern LINE_COMMENT = Pattern.compile("^#.*$");
  private static final Pattern WHITESPACE = Pattern.compile("\\s");
  private static final Pattern SPACES = Pattern.compile(" +");

  // phrase text -> phrase ID
  private final TokenInfoFST fst;

  // holds wordid, length, length... indexed by phrase ID
  private final int[][] segmentations;

  // holds readings and POS, indexed by wordid
  private final UserMorphData morphAtts;

  static final int CUSTOM_DICTIONARY_WORD_ID_OFFSET = 100000000;

  public static UserDictionary open(Reader reader) throws IOException {

    BufferedReader br = new BufferedReader(reader);
    String line;
    List<String[]> featureEntries = new ArrayList<>();

    // text, segmentation, readings, POS
    while ((line = br.readLine()) != null) {
      // Remove comments
      line = LINE_COMMENT.matcher(line).replaceAll("");

      // Skip empty lines or comment lines
      if (line.trim().isEmpty()) {
        continue;
      }
      String[] values = CSVUtil.parse(line);
      featureEntries.add(values);
    }

    if (featureEntries.isEmpty()) {
      return null;
    } else {
      return new UserDictionary(featureEntries);
    }
  }

  private UserDictionary(List<String[]> featureEntries) throws IOException {

    int wordId = CUSTOM_DICTIONARY_WORD_ID_OFFSET;
    // TODO: should we allow multiple segmentations per input 'phrase'?
    // the old treemap didn't support this either, and i'm not sure if it's needed/useful?

    featureEntries.sort((left, right) -> left[0].compareTo(right[0]));

    List<String> data = new ArrayList<>(featureEntries.size());
    List<int[]> segmentations = new ArrayList<>(featureEntries.size());

    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> fstCompiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE2, fstOutput).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = 0;

    for (String[] values : featureEntries) {
      String surface = WHITESPACE.matcher(values[0]).replaceAll("");
      String concatenatedSegment = WHITESPACE.matcher(values[1]).replaceAll("");
      String[] segmentation = SPACES.split(values[1]);
      String[] readings = SPACES.split(values[2]);
      String pos = values[3];

      if (segmentation.length != readings.length) {
        throw new RuntimeException(
            "Illegal user dictionary entry "
                + values[0]
                + " - the number of segmentations ("
                + segmentation.length
                + ")"
                + " does not the match number of readings ("
                + readings.length
                + ")");
      }

      if (!surface.equals(concatenatedSegment)) {
        throw new RuntimeException(
            "Illegal user dictionary entry "
                + values[0]
                + " - the concatenated segmentation ("
                + concatenatedSegment
                + ")"
                + " does not match the surface form ("
                + surface
                + ")");
      }

      int[] wordIdAndLength = new int[segmentation.length + 1]; // wordId offset, length, length....
      wordIdAndLength[0] = wordId;
      for (int i = 0; i < segmentation.length; i++) {
        wordIdAndLength[i + 1] = segmentation[i].length();
        data.add(readings[i] + INTERNAL_SEPARATOR + pos);
        wordId++;
      }
      // add mapping to FST
      String token = values[0];
      scratch.growNoCopy(token.length());
      scratch.setLength(token.length());
      for (int i = 0; i < token.length(); i++) {
        scratch.setIntAt(i, token.charAt(i));
      }
      fstCompiler.add(scratch.get(), ord);
      segmentations.add(wordIdAndLength);
      ord++;
    }
    this.fst =
        new TokenInfoFST(
            FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader()), false);
    this.morphAtts = new UserMorphData(data.toArray(new String[0]));
    this.segmentations = segmentations.toArray(new int[0][]);
  }

  @Override
  public UserMorphData getMorphAttributes() {
    return morphAtts;
  }

  /**
   * Lookup words in text
   *
   * @param chars text
   * @param off offset into text
   * @param len length of text
   * @return array of {wordId, position, length}
   */
  public int[][] lookup(char[] chars, int off, int len) throws IOException {
    List<Match> matches = null;
    int numResults = 0;
    final FST.BytesReader fstReader = fst.getBytesReader();
    final int end = off + len;
    FST.Arc<Long> arc = new FST.Arc<>();
    for (int startOffset = off; startOffset < end; startOffset++) {
      int[] wordIdAndLength = null;
      arc = fst.getFirstArc(arc);
      int output = 0;
      for (int i = 0, remaining = end - startOffset; i < remaining; i++) {
        int ch = chars[startOffset + i];
        if (fst.findTargetArc(ch, arc, arc, i == 0, fstReader) == null) {
          break; // continue to next position
        }
        output += arc.output().intValue();
        if (arc.isFinal()) {
          int finalOutput = output + arc.nextFinalOutput().intValue();
          wordIdAndLength = segmentations[finalOutput];
        }
      }
      if (wordIdAndLength != null) {
        if (matches == null) {
          matches = new ArrayList<>();
        }
        matches.add(new Match(startOffset - off, wordIdAndLength));
        numResults += wordIdAndLength.length - 1;
      }
    }
    if (numResults == 0) {
      return EMPTY_RESULT;
    }
    int[][] result = new int[numResults][];
    int index = 0;
    for (int i = 0; i < matches.size(); i++) {
      Match match = matches.get(i);
      int[] wordIdAndLength = match.wordIdAndLength;
      int wordId = wordIdAndLength[0];
      // convert length to index
      int position = match.position;
      for (int j = 1; j < wordIdAndLength.length; j++) { // first entry is wordId offset
        // add a {wordId, index, length} token to the results
        int[] token = {wordId + j - 1, position, wordIdAndLength[j]};
        result[index++] = token;
        position += wordIdAndLength[j];
      }
    }
    return result;
  }

  public TokenInfoFST getFST() {
    return fst;
  }

  private static final int[][] EMPTY_RESULT = new int[0][];

  public int[] lookupSegmentation(int phraseID) {
    return segmentations[phraseID];
  }

  private record Match(int position, int[] wordIdAndLength) {}
}
