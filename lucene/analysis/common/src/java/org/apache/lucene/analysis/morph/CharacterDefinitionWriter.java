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
package org.apache.lucene.analysis.morph;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

/** Writes character definition file */
public final class CharacterDefinitionWriter<T extends CharacterDefinition> {

  private final Class<T> implClazz;

  private final byte[] characterCategoryMap = new byte[0x10000];
  private final int classCount;
  private final boolean[] invokeMap;
  private final boolean[] groupMap;
  private final CharacterDefinition.LookupCharacterClass lookupCharClass;

  /** Constructor for building. TODO: remove write access */
  public CharacterDefinitionWriter(
      Class<T> implClazz,
      byte defaultValue,
      int classCount,
      CharacterDefinition.LookupCharacterClass lookupCharClass) {
    this.implClazz = implClazz;
    Arrays.fill(characterCategoryMap, defaultValue);
    this.invokeMap = new boolean[classCount];
    this.groupMap = new boolean[classCount];
    this.classCount = classCount;
    this.lookupCharClass = lookupCharClass;
  }

  /**
   * Put mapping from unicode code point to character class.
   *
   * @param codePoint code point
   * @param characterClassName character class name
   */
  public void putCharacterCategory(int codePoint, String characterClassName) {
    characterClassName = characterClassName.split(" ")[0]; // use first
    // category
    // class

    // Override Nakaguro
    if (codePoint == 0x30FB) {
      characterClassName = "SYMBOL";
    }
    characterCategoryMap[codePoint] = lookupCharClass.lookupCharacterClass(characterClassName);
  }

  public void putInvokeDefinition(String characterClassName, int invoke, int group, int length) {
    final byte characterClass = lookupCharClass.lookupCharacterClass(characterClassName);
    invokeMap[characterClass] = invoke == 1;
    groupMap[characterClass] = group == 1;
    // TODO: length def ignored
  }

  private String getBaseFileName() {
    return implClazz.getName().replace('.', '/');
  }

  public void write(Path baseDir, String charDefCodecHeader, int charDefCodecVersion)
      throws IOException {
    Path path = baseDir.resolve(getBaseFileName() + CharacterDefinition.FILENAME_SUFFIX);
    Files.createDirectories(path.getParent());
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, charDefCodecHeader, charDefCodecVersion);
      out.writeBytes(characterCategoryMap, 0, characterCategoryMap.length);
      for (int i = 0; i < classCount; i++) {
        final byte b = (byte) ((invokeMap[i] ? 0x01 : 0x00) | (groupMap[i] ? 0x02 : 0x00));
        out.writeByte(b);
      }
    }
  }
}
