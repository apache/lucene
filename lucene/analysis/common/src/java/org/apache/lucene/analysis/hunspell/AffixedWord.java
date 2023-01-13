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
package org.apache.lucene.analysis.hunspell;

import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_FLAG;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An object representing the analysis result of a simple (non-compound) word */
public final class AffixedWord {
  private final String word;
  private final DictEntry entry;
  private final List<Affix> prefixes;
  private final List<Affix> suffixes;

  AffixedWord(String word, DictEntry entry, List<Affix> prefixes, List<Affix> suffixes) {
    this.word = word;
    this.entry = entry;
    this.prefixes = Collections.unmodifiableList(prefixes);
    this.suffixes = Collections.unmodifiableList(suffixes);
  }

  /**
   * @return the word being analyzed
   */
  public String getWord() {
    return word;
  }

  /**
   * @return the dictionary entry for the stem in this analysis
   */
  public DictEntry getDictEntry() {
    return entry;
  }

  /**
   * @return the list of prefixes applied to the stem, at most two, outermost first
   */
  public List<Affix> getPrefixes() {
    return prefixes;
  }

  /**
   * @return the list of suffixes applied to the stem, at most two, outermost first
   */
  public List<Affix> getSuffixes() {
    return suffixes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AffixedWord)) return false;
    AffixedWord that = (AffixedWord) o;
    return word.equals(that.word)
        && entry.equals(that.entry)
        && prefixes.equals(that.prefixes)
        && suffixes.equals(that.suffixes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(word, entry, prefixes, suffixes);
  }

  @Override
  public String toString() {
    return "AffixedWord["
        + ("word=" + word + ", ")
        + ("entry=" + entry + ", ")
        + ("prefixes=" + prefixes + ", ")
        + ("suffixes=" + suffixes)
        + "]";
  }

  /** An object representing a prefix or a suffix applied to a word stem */
  public static final class Affix {
    final int affixId;
    private final String presentableFlag;

    Affix(Dictionary dictionary, int affixId) {
      this.affixId = affixId;
      char encodedFlag = dictionary.affixData(affixId, AFFIX_FLAG);
      presentableFlag = dictionary.flagParsingStrategy.printFlag(encodedFlag);
    }

    /**
     * @return the corresponding affix flag as it appears in the *.aff file. Depending on the
     *     format, it could be a Unicode character, two ASCII characters, or an integer in decimal
     *     form
     */
    public String getFlag() {
      return presentableFlag;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || o instanceof Affix && affixId == ((Affix) o).affixId;
    }

    @Override
    public int hashCode() {
      return affixId;
    }

    @Override
    public String toString() {
      return presentableFlag + "(id=" + affixId + ")";
    }
  }
}
