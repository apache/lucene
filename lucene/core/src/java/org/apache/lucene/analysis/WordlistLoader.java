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
package org.apache.lucene.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.IOUtils;

/**
 * Loader for text files that represent a list of stopwords.
 *
 * @see IOUtils to obtain {@link Reader} instances
 * @lucene.internal
 */
public class WordlistLoader {

  private static final int INITIAL_CAPACITY = 16;

  /** no instance */
  private WordlistLoader() {}

  /**
   * Reads lines from a Reader and adds every non-blank line as an entry to a CharArraySet (omitting
   * leading and trailing whitespace). Every line of the Reader should contain only one word. The
   * words need to be in lowercase if you make use of an Analyzer which uses LowerCaseFilter (like
   * StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @param result the {@link CharArraySet} to fill with the readers words
   * @return the given {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getWordSet(Reader reader, CharArraySet result) throws IOException {
    try (BufferedReader br = getBufferedReader(reader)) {
      String word = null;
      while ((word = br.readLine()) != null) {
        word = word.trim();
        // skip blank lines
        if (word.isEmpty()) continue;
        result.add(word);
      }
    }
    return result;
  }

  /**
   * Reads lines from a Reader and adds every line as an entry to a CharArraySet (omitting leading
   * and trailing whitespace). Every line of the Reader should contain only one word. The words need
   * to be in lowercase if you make use of an Analyzer which uses LowerCaseFilter (like
   * StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getWordSet(Reader reader) throws IOException {
    return CharArraySet.unmodifiableSet(
        getWordSet(reader, new CharArraySet(INITIAL_CAPACITY, false)));
  }

  /**
   * Reads lines from an InputStream with UTF-8 charset and adds every line as an entry to a
   * CharArraySet (omitting leading and trailing whitespace). Every line of the Reader should
   * contain only one word. The words need to be in lowercase if you make use of an Analyzer which
   * uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param stream InputStream containing the wordlist
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getWordSet(InputStream stream) throws IOException {
    return getWordSet(stream, StandardCharsets.UTF_8);
  }

  /**
   * Reads lines from an InputStream with the given charset and adds every line as an entry to a
   * CharArraySet (omitting leading and trailing whitespace). Every line of the Reader should
   * contain only one word. The words need to be in lowercase if you make use of an Analyzer which
   * uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param stream InputStream containing the wordlist
   * @param charset Charset of the wordlist
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getWordSet(InputStream stream, Charset charset) throws IOException {
    return getWordSet(IOUtils.getDecodingReader(stream, charset));
  }

  /**
   * Reads lines from a Reader and adds every non-blank non-comment line as an entry to a
   * CharArraySet (omitting leading and trailing whitespace). Every line of the Reader should
   * contain only one word. The words need to be in lowercase if you make use of an Analyzer which
   * uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @param comment The string representing a comment.
   * @param result the {@link CharArraySet} to fill with the readers words
   * @return the given {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getWordSet(Reader reader, String comment, CharArraySet result)
      throws IOException {
    try (BufferedReader br = getBufferedReader(reader)) {
      String word = null;
      while ((word = br.readLine()) != null) {
        if (word.startsWith(comment) == false) {
          word = word.trim();
          // skip blank lines
          if (word.isEmpty()) continue;
          result.add(word);
        }
      }
    }
    return result;
  }

  /**
   * Reads lines from a Reader and adds every non-comment line as an entry to a CharArraySet
   * (omitting leading and trailing whitespace). Every line of the Reader should contain only one
   * word. The words need to be in lowercase if you make use of an Analyzer which uses
   * LowerCaseFilter (like StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @param comment The string representing a comment.
   * @return An unmodifiable CharArraySet with the reader's words
   */
  public static CharArraySet getWordSet(Reader reader, String comment) throws IOException {
    return CharArraySet.unmodifiableSet(
        getWordSet(reader, comment, new CharArraySet(INITIAL_CAPACITY, false)));
  }

  /**
   * Reads lines from an InputStream with UTF-8 charset and adds every non-comment line as an entry
   * to a CharArraySet (omitting leading and trailing whitespace). Every line of the Reader should
   * contain only one word. The words need to be in lowercase if you make use of an Analyzer which
   * uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param stream InputStream in UTF-8 encoding containing the wordlist
   * @param comment The string representing a comment.
   * @return An unmodifiable CharArraySet with the reader's words
   */
  public static CharArraySet getWordSet(InputStream stream, String comment) throws IOException {
    return getWordSet(stream, StandardCharsets.UTF_8, comment);
  }

  /**
   * Reads lines from an InputStream with the given charset and adds every non-comment line as an
   * entry to a CharArraySet (omitting leading and trailing whitespace). Every line of the Reader
   * should contain only one word. The words need to be in lowercase if you make use of an Analyzer
   * which uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param stream InputStream containing the wordlist
   * @param charset Charset of the wordlist
   * @param comment The string representing a comment.
   * @return An unmodifiable CharArraySet with the reader's words
   */
  public static CharArraySet getWordSet(InputStream stream, Charset charset, String comment)
      throws IOException {
    return getWordSet(IOUtils.getDecodingReader(stream, charset), comment);
  }

  /**
   * Reads stopwords from a stopword list in Snowball format.
   *
   * <p>The snowball format is the following:
   *
   * <ul>
   *   <li>Lines may contain multiple words separated by whitespace.
   *   <li>The comment character is the vertical line (&#124;).
   *   <li>Lines may contain trailing comments.
   * </ul>
   *
   * @param reader Reader containing a Snowball stopword list
   * @param result the {@link CharArraySet} to fill with the readers words
   * @return the given {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getSnowballWordSet(Reader reader, CharArraySet result)
      throws IOException {
    try (BufferedReader br = getBufferedReader(reader)) {
      String line = null;
      while ((line = br.readLine()) != null) {
        int comment = line.indexOf('|');
        if (comment >= 0) line = line.substring(0, comment);
        String[] words = line.split("\\s+");
        for (int i = 0; i < words.length; i++) {
          if (words[i].length() > 0) result.add(words[i]);
        }
      }
    }
    return result;
  }

  /**
   * Reads stopwords from a stopword list in Snowball format.
   *
   * <p>The snowball format is the following:
   *
   * <ul>
   *   <li>Lines may contain multiple words separated by whitespace.
   *   <li>The comment character is the vertical line (&#124;).
   *   <li>Lines may contain trailing comments.
   * </ul>
   *
   * @param reader Reader containing a Snowball stopword list
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getSnowballWordSet(Reader reader) throws IOException {
    return CharArraySet.unmodifiableSet(
        getSnowballWordSet(reader, new CharArraySet(INITIAL_CAPACITY, false)));
  }

  /**
   * Reads stopwords from a stopword list in Snowball format.
   *
   * <p>The snowball format is the following:
   *
   * <ul>
   *   <li>Lines may contain multiple words separated by whitespace.
   *   <li>The comment character is the vertical line (&#124;).
   *   <li>Lines may contain trailing comments.
   * </ul>
   *
   * @param stream InputStream in UTF-8 encoding containing a Snowball stopword list
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getSnowballWordSet(InputStream stream) throws IOException {
    return getSnowballWordSet(stream, StandardCharsets.UTF_8);
  }

  /**
   * Reads stopwords from a stopword list in Snowball format.
   *
   * <p>The snowball format is the following:
   *
   * <ul>
   *   <li>Lines may contain multiple words separated by whitespace.
   *   <li>The comment character is the vertical line (&#124;).
   *   <li>Lines may contain trailing comments.
   * </ul>
   *
   * @param stream InputStream containing a Snowball stopword list
   * @param charset Charset of the stopword list
   * @return An unmodifiable {@link CharArraySet} with the reader's words
   */
  public static CharArraySet getSnowballWordSet(InputStream stream, Charset charset)
      throws IOException {
    return getSnowballWordSet(IOUtils.getDecodingReader(stream, charset));
  }

  /**
   * Reads a stem dictionary. Each line contains:
   *
   * <pre>word<b>\t</b>stem</pre>
   *
   * (i.e. two tab separated words)
   *
   * @return stem dictionary that overrules the stemming algorithm
   * @throws IOException If there is a low-level I/O error.
   */
  public static CharArrayMap<String> getStemDict(Reader reader, CharArrayMap<String> result)
      throws IOException {
    try (BufferedReader br = getBufferedReader(reader)) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] wordstem = line.split("\t", 2);
        result.put(wordstem[0], wordstem[1]);
      }
    }
    return result;
  }

  /**
   * Accesses a resource by name and returns the (non comment) lines containing data using the given
   * character encoding.
   *
   * <p>A comment line is any line that starts with the character "#"
   *
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * @throws IOException If there is a low-level I/O error.
   */
  public static List<String> getLines(InputStream stream, Charset charset) throws IOException {
    ArrayList<String> lines;
    try (BufferedReader input = getBufferedReader(IOUtils.getDecodingReader(stream, charset))) {
      lines = new ArrayList<>();
      for (String word = null; (word = input.readLine()) != null; ) {
        // skip initial bom marker
        if (lines.isEmpty() && word.length() > 0 && word.charAt(0) == '\uFEFF')
          word = word.substring(1);
        // skip comments
        if (word.startsWith("#")) continue;
        word = word.trim();
        // skip blank lines
        if (word.length() == 0) continue;
        lines.add(word);
      }
      return lines;
    }
  }

  private static BufferedReader getBufferedReader(Reader reader) {
    return (reader instanceof BufferedReader)
        ? (BufferedReader) reader
        : new BufferedReader(reader);
  }
}
