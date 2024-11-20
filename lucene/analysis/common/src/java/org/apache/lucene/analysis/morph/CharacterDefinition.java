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

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;

/** Character category data. */
public abstract class CharacterDefinition {

  public static final String FILENAME_SUFFIX = ".dat";

  protected final byte[] characterCategoryMap = new byte[0x10000];
  private final boolean[] invokeMap;
  private final boolean[] groupMap;

  protected CharacterDefinition(
      IOSupplier<InputStream> charDefResource,
      String charDefCodecHeader,
      int charDefCodecVersion,
      int classCount)
      throws IOException {
    try (InputStream is = charDefResource.get()) {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, charDefCodecHeader, charDefCodecVersion, charDefCodecVersion);
      in.readBytes(characterCategoryMap, 0, characterCategoryMap.length);
      this.invokeMap = new boolean[classCount];
      this.groupMap = new boolean[classCount];
      for (int i = 0; i < classCount; i++) {
        final byte b = in.readByte();
        invokeMap[i] = (b & 0x01) != 0;
        groupMap[i] = (b & 0x02) != 0;
      }
    }
  }

  public byte getCharacterClass(char c) {
    return characterCategoryMap[c];
  }

  public boolean isInvoke(char c) {
    return invokeMap[characterCategoryMap[c]];
  }

  public boolean isGroup(char c) {
    return groupMap[characterCategoryMap[c]];
  }

  /** Functional interface to lookup character class */
  @FunctionalInterface
  public interface LookupCharacterClass {
    /** looks up character class for given class name */
    byte lookupCharacterClass(String characterClassName);
  }
}
