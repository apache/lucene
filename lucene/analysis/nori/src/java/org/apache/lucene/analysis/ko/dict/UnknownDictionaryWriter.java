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
import java.nio.file.Path;
import org.apache.lucene.analysis.morph.BinaryDictionaryWriter;
import org.apache.lucene.analysis.morph.CharacterDefinitionWriter;

class UnknownDictionaryWriter extends BinaryDictionaryWriter<UnknownDictionary> {

  private final org.apache.lucene.analysis.morph.CharacterDefinitionWriter<CharacterDefinition>
      characterDefinition =
          new CharacterDefinitionWriter<>(
              CharacterDefinition.class,
              CharacterDefinition.DEFAULT,
              CharacterDefinition.CLASS_COUNT,
              CharacterDefinition::lookupCharacterClass);

  public UnknownDictionaryWriter(int size) {
    super(UnknownDictionary.class, new TokenInfoDictionaryEntryWriter(size));
  }

  @Override
  public int put(String[] entry) {
    // Get wordId of current entry
    int wordId = entryWriter.currentPosition();

    // Put entry
    int result = super.put(entry);

    // Put entry in targetMap
    int characterId = CharacterDefinition.lookupCharacterClass(entry[0]);
    addMapping(characterId, wordId);
    return result;
  }

  /**
   * Put mapping from unicode code point to character class.
   *
   * @param codePoint code point
   * @param characterClassName character class name
   */
  public void putCharacterCategory(int codePoint, String characterClassName) {
    characterDefinition.putCharacterCategory(codePoint, characterClassName);
  }

  public void putInvokeDefinition(String characterClassName, int invoke, int group, int length) {
    characterDefinition.putInvokeDefinition(characterClassName, invoke, group, length);
  }

  @Override
  public void write(Path baseDir) throws IOException {
    super.write(
        baseDir,
        DictionaryConstants.TARGETMAP_HEADER,
        DictionaryConstants.POSDICT_HEADER,
        DictionaryConstants.DICT_HEADER,
        DictionaryConstants.VERSION);
    characterDefinition.write(
        baseDir, DictionaryConstants.CHARDEF_HEADER, DictionaryConstants.VERSION);
  }
}
