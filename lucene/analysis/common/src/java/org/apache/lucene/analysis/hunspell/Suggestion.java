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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

class Suggestion {
  final String raw;
  final String[] result;

  Suggestion(String raw, String misspelled, WordCase originalCase, Hunspell speller) {
    this.raw = raw;

    List<String> result = new ArrayList<>();
    String adjusted = adjustSuggestionCase(raw, misspelled, originalCase);
    result.add(
        cleanOutput(speller, adjusted.contains(" ") || speller.spell(adjusted) ? adjusted : raw));
    if (originalCase == WordCase.UPPER && speller.dictionary.checkSharpS && raw.contains("ß")) {
      result.add(cleanOutput(speller, raw));
    }
    this.result = result.toArray(new String[0]);
  }

  private String adjustSuggestionCase(String candidate, String misspelled, WordCase originalCase) {
    if (originalCase == WordCase.UPPER) {
      return candidate.toUpperCase(Locale.ROOT);
    }
    if (Character.isUpperCase(misspelled.charAt(0))) {
      return Character.toUpperCase(candidate.charAt(0)) + candidate.substring(1);
    }
    return candidate;
  }

  private String cleanOutput(Hunspell speller, String s) {
    if (speller.dictionary.oconv == null) return s;

    StringBuilder sb = new StringBuilder(s);
    speller.dictionary.oconv.applyMappings(sb);
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Suggestion)) return false;
    Suggestion that = (Suggestion) o;
    return raw.equals(that.raw) && Arrays.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hash(raw) + Arrays.hashCode(result);
  }

  @Override
  public String toString() {
    return raw;
  }
}
