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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** An object representing *.dic file entry with its word, flags and morphological data. */
public abstract class DictEntry {
  private final String stem;

  DictEntry(String stem) {
    this.stem = stem;
  }

  @Override
  public String toString() {
    String result = stem;
    String flags = getFlags();
    if (!flags.isEmpty()) {
      result += "/" + flags;
    }
    String morph = getMorphologicalData();
    if (!morph.isEmpty()) {
      result += " " + morph;
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DictEntry)) return false;
    DictEntry that = (DictEntry) o;
    return stem.equals(that.stem)
        && getMorphologicalData().equals(that.getMorphologicalData())
        && getFlags().equals(that.getFlags());
  }

  @Override
  public int hashCode() {
    return Objects.hash(stem, getFlags(), getMorphologicalData());
  }

  /**
   * @return the stem word in the dictionary
   */
  public String getStem() {
    return stem;
  }

  /**
   * @return the flags associated with the dictionary entry, encoded in the same format as in the
   *     *.dic file, but possibly in a different order
   */
  public abstract String getFlags();

  /**
   * @return morphological fields (of {@code kk:vvvvvv} form, sorted, space-separated, excluding
   *     {@code ph:}) associated with the homonym at the given entry index, or an empty string
   */
  public abstract String getMorphologicalData();

  /**
   * @param key the key in the form {@code kk:} by which to filter the morphological fields
   * @return the values (of {@code vvvvvv} form) of morphological fields with the given key
   *     associated with the homonym at the given entry index
   */
  public List<String> getMorphologicalValues(String key) {
    assert key.length() == 3 && key.charAt(2) == ':'
        : "A morphological data key should consist of two letters followed by a semicolon, found: "
            + key;

    String data = getMorphologicalData();
    if (data.isEmpty() || !data.contains(key)) return Collections.emptyList();

    return Arrays.stream(data.split(" "))
        .filter(s -> s.startsWith(key))
        .map(s -> s.substring(3))
        .collect(Collectors.toList());
  }

  static DictEntry create(String stem, String flags) {
    return new DictEntry(stem) {
      @Override
      public String getFlags() {
        return flags;
      }

      @Override
      public String getMorphologicalData() {
        return "";
      }
    };
  }
}
