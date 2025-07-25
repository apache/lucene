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
package org.apache.lucene.analysis.miscellaneous;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_ALL;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_NUMBERS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_WORDS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.GENERATE_WORD_PARTS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.IGNORE_KEYWORDS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.PRESERVE_ORIGINAL;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.SPLIT_ON_NUMERICS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.ALPHA;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.ALPHANUM;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.DIGIT;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.LOWER;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.SUBWORD_DELIM;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.UPPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link WordDelimiterGraphFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_wd" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.WordDelimiterGraphFilterFactory" protected="protectedword.txt"
 *             preserveOriginal="0" splitOnNumerics="1" splitOnCaseChange="1"
 *             catenateWords="0" catenateNumbers="0" catenateAll="0"
 *             generateWordParts="1" generateNumberParts="1" stemEnglishPossessive="1"
 *             types="wdfftypes.txt" ignoreKeywords="0" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 6.5.0
 * @lucene.spi {@value #NAME}
 */
public class WordDelimiterGraphFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "wordDelimiterGraph";

  public static final String PROTECTED_TOKENS = "protected";
  public static final String TYPES = "types";
  public static final String OFFSETS = "adjustOffsets";

  private final String wordFiles;
  private final String types;
  private final int flags;
  byte[] typeTable = null;
  private CharArraySet protectedWords = null;
  private final boolean adjustOffsets;

  /** Creates a new WordDelimiterGraphFilterFactory */
  public WordDelimiterGraphFilterFactory(Map<String, String> args) {
    super(args);
    int flags = 0;
    if (getInt(args, "generateWordParts", 1) != 0) {
      flags |= GENERATE_WORD_PARTS;
    }
    if (getInt(args, "generateNumberParts", 1) != 0) {
      flags |= GENERATE_NUMBER_PARTS;
    }
    if (getInt(args, "catenateWords", 0) != 0) {
      flags |= CATENATE_WORDS;
    }
    if (getInt(args, "catenateNumbers", 0) != 0) {
      flags |= CATENATE_NUMBERS;
    }
    if (getInt(args, "catenateAll", 0) != 0) {
      flags |= CATENATE_ALL;
    }
    if (getInt(args, "splitOnCaseChange", 1) != 0) {
      flags |= SPLIT_ON_CASE_CHANGE;
    }
    if (getInt(args, "splitOnNumerics", 1) != 0) {
      flags |= SPLIT_ON_NUMERICS;
    }
    if (getInt(args, "preserveOriginal", 0) != 0) {
      flags |= PRESERVE_ORIGINAL;
    }
    if (getInt(args, "stemEnglishPossessive", 1) != 0) {
      flags |= STEM_ENGLISH_POSSESSIVE;
    }
    if (getInt(args, "ignoreKeywords", 0) != 0) {
      flags |= IGNORE_KEYWORDS;
    }
    wordFiles = get(args, PROTECTED_TOKENS);
    types = get(args, TYPES);
    this.flags = flags;
    this.adjustOffsets = getBoolean(args, OFFSETS, true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public WordDelimiterGraphFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (wordFiles != null) {
      protectedWords = getWordSet(loader, wordFiles, false);
    }
    if (types != null) {
      List<String> files = splitFileNames(types);
      List<String> wlist = new ArrayList<>();
      for (String file : files) {
        List<String> lines = getLines(loader, file.trim());
        wlist.addAll(lines);
      }
      typeTable = parseTypes(wlist);
    }
  }

  @Override
  public TokenFilter create(TokenStream input) {
    return new WordDelimiterGraphFilter(
        input,
        adjustOffsets,
        typeTable == null ? WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE : typeTable,
        flags,
        protectedWords);
  }

  // source => type
  private static final Pattern TYPE_PATTERN = Pattern.compile("(.*)\\s*=>\\s*(.*)\\s*$");

  // parses a list of MappingCharFilter style rules into a custom byte[] type table
  private byte[] parseTypes(List<String> rules) {
    SortedMap<Character, Byte> typeMap = new TreeMap<>();
    for (String rule : rules) {
      Matcher m = TYPE_PATTERN.matcher(rule);
      if (!m.find()) throw new IllegalArgumentException("Invalid Mapping Rule : [" + rule + "]");
      String lhs = parseString(m.group(1).trim());
      Byte rhs = parseType(m.group(2).trim());
      if (lhs.length() != 1)
        throw new IllegalArgumentException(
            "Invalid Mapping Rule : [" + rule + "]. Only a single character is allowed.");
      if (rhs == null)
        throw new IllegalArgumentException("Invalid Mapping Rule : [" + rule + "]. Illegal type.");
      typeMap.put(lhs.charAt(0), rhs);
    }

    // ensure the table is always at least as big as DEFAULT_WORD_DELIM_TABLE for performance
    byte[] types =
        new byte
            [Math.max(
                typeMap.lastKey() + 1, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE.length)];
    for (int i = 0; i < types.length; i++) types[i] = WordDelimiterIterator.getType(i);
    for (Map.Entry<Character, Byte> mapping : typeMap.entrySet())
      types[mapping.getKey()] = mapping.getValue();
    return types;
  }

  private Byte parseType(String s) {
    if (s.equals("LOWER")) return LOWER;
    else if (s.equals("UPPER")) return UPPER;
    else if (s.equals("ALPHA")) return ALPHA;
    else if (s.equals("DIGIT")) return DIGIT;
    else if (s.equals("ALPHANUM")) return ALPHANUM;
    else if (s.equals("SUBWORD_DELIM")) return SUBWORD_DELIM;
    else return null;
  }

  char[] out = new char[256];

  private String parseString(String s) {
    int readPos = 0;
    int len = s.length();
    int writePos = 0;
    while (readPos < len) {
      char c = s.charAt(readPos++);
      if (c == '\\') {
        if (readPos >= len)
          throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
        c = s.charAt(readPos++);
        switch (c) {
          case '\\':
            c = '\\';
            break;
          case 'n':
            c = '\n';
            break;
          case 't':
            c = '\t';
            break;
          case 'r':
            c = '\r';
            break;
          case 'b':
            c = '\b';
            break;
          case 'f':
            c = '\f';
            break;
          case 'u':
            if (readPos + 3 >= len)
              throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
            c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
            readPos += 4;
            break;
        }
      }
      out[writePos++] = c;
    }
    return new String(out, 0, writePos);
  }
}
