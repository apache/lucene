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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.util.IOUtils;

/** Character category data. */
public final class CharacterDefinition
    extends org.apache.lucene.analysis.morph.CharacterDefinition {

  public static final int CLASS_COUNT = CharacterClass.values().length;

  // only used internally for lookup:
  enum CharacterClass {
    NGRAM,
    DEFAULT,
    SPACE,
    SYMBOL,
    NUMERIC,
    ALPHA,
    CYRILLIC,
    GREEK,
    HIRAGANA,
    KATAKANA,
    KANJI,
    HANGUL,
    HANJA,
    HANJANUMERIC
  }

  // the classes:
  public static final byte NGRAM = (byte) CharacterClass.NGRAM.ordinal();
  public static final byte DEFAULT = (byte) CharacterClass.DEFAULT.ordinal();
  public static final byte SPACE = (byte) CharacterClass.SPACE.ordinal();
  public static final byte SYMBOL = (byte) CharacterClass.SYMBOL.ordinal();
  public static final byte NUMERIC = (byte) CharacterClass.NUMERIC.ordinal();
  public static final byte ALPHA = (byte) CharacterClass.ALPHA.ordinal();
  public static final byte CYRILLIC = (byte) CharacterClass.CYRILLIC.ordinal();
  public static final byte GREEK = (byte) CharacterClass.GREEK.ordinal();
  public static final byte HIRAGANA = (byte) CharacterClass.HIRAGANA.ordinal();
  public static final byte KATAKANA = (byte) CharacterClass.KATAKANA.ordinal();
  public static final byte KANJI = (byte) CharacterClass.KANJI.ordinal();
  public static final byte HANGUL = (byte) CharacterClass.HANGUL.ordinal();
  public static final byte HANJA = (byte) CharacterClass.HANJA.ordinal();
  public static final byte HANJANUMERIC = (byte) CharacterClass.HANJANUMERIC.ordinal();

  private CharacterDefinition() throws IOException {
    super(
        CharacterDefinition::getClassResource,
        DictionaryConstants.CHARDEF_HEADER,
        DictionaryConstants.VERSION,
        CharacterDefinition.CLASS_COUNT);
  }

  private static InputStream getClassResource() throws IOException {
    final String resourcePath = CharacterDefinition.class.getSimpleName() + FILENAME_SUFFIX;
    return IOUtils.requireResourceNonNull(
        CharacterDefinition.class.getResourceAsStream(resourcePath), resourcePath);
  }

  public boolean isHanja(char c) {
    final byte characterClass = getCharacterClass(c);
    return characterClass == HANJA || characterClass == HANJANUMERIC;
  }

  public boolean isHangul(char c) {
    return getCharacterClass(c) == HANGUL;
  }

  public boolean hasCoda(char ch) {
    return ((ch - 0xAC00) % 0x001C) != 0;
  }

  public static byte lookupCharacterClass(String characterClassName) {
    return (byte) CharacterClass.valueOf(characterClassName).ordinal();
  }

  public static CharacterDefinition getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private static class SingletonHolder {
    static final CharacterDefinition INSTANCE;

    static {
      try {
        INSTANCE = new CharacterDefinition();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load CharacterDefinition.", ioe);
      }
    }
  }
}
