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

import static org.apache.lucene.analysis.ja.dict.BinaryDictionary.DICT_FILENAME_SUFFIX;
import static org.apache.lucene.analysis.ja.dict.BinaryDictionary.POSDICT_FILENAME_SUFFIX;
import static org.apache.lucene.analysis.ja.dict.BinaryDictionary.TARGETMAP_FILENAME_SUFFIX;
import static org.apache.lucene.analysis.ja.dict.TokenInfoDictionary.FST_FILENAME_SUFFIX;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.analysis.ja.util.DictionaryBuilder;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;

public class TestExternalDictionary extends LuceneTestCase {

  private Path dir;
  private ClassLoader loader = getClass().getClassLoader();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = createTempDir("systemDict");
    try (BufferedWriter writer =
        Files.newBufferedWriter(dir.resolve("unk.def"), StandardCharsets.UTF_8)) {
      writer.write("DEFAULT,5,5,4769,記号,一般,*,*,*,*,*");
      writer.newLine();
      writer.write("SPACE,9,9,8903,記号,空白,*,*,*,*,*");
      writer.newLine();
    }
    try (BufferedWriter writer =
        Files.newBufferedWriter(dir.resolve("char.def"), StandardCharsets.UTF_8)) {
      writer.write("0x0021..0x002F SYMBOL");
      writer.newLine();
      writer.write("0x0030..0x0039 NUMERIC");
      writer.newLine();
    }
    try (BufferedWriter writer =
        Files.newBufferedWriter(dir.resolve("matrix.def"), StandardCharsets.UTF_8)) {
      writer.write("3 3");
      writer.newLine();
      writer.write("0 1 1");
      writer.newLine();
      writer.write("0 2 -1630");
      writer.newLine();
    }
    try (BufferedWriter writer =
        Files.newBufferedWriter(dir.resolve("noun.csv"), StandardCharsets.UTF_8)) {
      writer.write("白昼夢,1285,1285,5622,名詞,一般,*,*,*,*,白昼夢,ハクチュウム,ハクチューム");
      writer.newLine();
      writer.write("デバッギング,1285,1285,3657,名詞,一般,*,*,*,*,デバッギング,デバッギング,デバッギング");
      writer.newLine();
    }
    DictionaryBuilder.build(DictionaryBuilder.DictionaryFormat.IPADIC, dir, dir, "utf-8", true);
  }

  public void testLoadExternalTokenInfoDictionary() throws Exception {
    String dictionaryPath = TokenInfoDictionary.class.getName().replace('.', '/');
    TokenInfoDictionary dict =
        new TokenInfoDictionary(
            dir.resolve(dictionaryPath + TARGETMAP_FILENAME_SUFFIX),
            dir.resolve(dictionaryPath + POSDICT_FILENAME_SUFFIX),
            dir.resolve(dictionaryPath + DICT_FILENAME_SUFFIX),
            dir.resolve(dictionaryPath + FST_FILENAME_SUFFIX));
    assertNotNull(dict.getFST());
  }

  public void testLoadExternalUnknownDictionary() throws Exception {
    String dictionaryPath = UnknownDictionary.class.getName().replace('.', '/');
    UnknownDictionary dict =
        new UnknownDictionary(
            dir.resolve(dictionaryPath + TARGETMAP_FILENAME_SUFFIX),
            dir.resolve(dictionaryPath + POSDICT_FILENAME_SUFFIX),
            dir.resolve(dictionaryPath + DICT_FILENAME_SUFFIX));
    assertNotNull(dict.getCharacterDefinition());
  }

  public void testLoadExternalConnectionCosts() throws Exception {
    String dictionaryPath = ConnectionCosts.class.getName().replace('.', '/');
    ConnectionCosts cc =
        new ConnectionCosts(dir.resolve(dictionaryPath + ConnectionCosts.FILENAME_SUFFIX));
    assertEquals(1, cc.get(0, 1));
  }

  public void testLoadExternalUrlTokenInfoDictionary() throws Exception {
    String dictionaryPath = TokenInfoDictionary.class.getName().replace('.', '/');
    TokenInfoDictionary dict =
        new TokenInfoDictionary(
            loader.getResource(dictionaryPath + TARGETMAP_FILENAME_SUFFIX),
            loader.getResource(dictionaryPath + POSDICT_FILENAME_SUFFIX),
            loader.getResource(dictionaryPath + DICT_FILENAME_SUFFIX),
            loader.getResource(dictionaryPath + FST_FILENAME_SUFFIX));
    assertNotNull(dict.getFST());
  }

  public void testLoadExternalUrlUnknownDictionary() throws Exception {
    String dictionaryPath = UnknownDictionary.class.getName().replace('.', '/');
    UnknownDictionary dict =
        new UnknownDictionary(
            loader.getResource(dictionaryPath + TARGETMAP_FILENAME_SUFFIX),
            loader.getResource(dictionaryPath + POSDICT_FILENAME_SUFFIX),
            loader.getResource(dictionaryPath + DICT_FILENAME_SUFFIX));
    assertNotNull(dict.getCharacterDefinition());
  }

  public void testLoadExternalUrlConnectionCosts() throws Exception {
    String dictionaryPath = ConnectionCosts.class.getName().replace('.', '/');
    ConnectionCosts cc =
        new ConnectionCosts(loader.getResource(dictionaryPath + ConnectionCosts.FILENAME_SUFFIX));
    assertEquals(1, cc.get(0, 1));
  }

  @Deprecated(forRemoval = true, since = "9.1")
  @SuppressWarnings("removal")
  public void testDeprecatedLoadExternalTokenInfoDictionary() throws Exception {
    String dictionaryPath = TokenInfoDictionary.class.getName().replace('.', '/');
    TokenInfoDictionary dict =
        new TokenInfoDictionary(BinaryDictionary.ResourceScheme.CLASSPATH, dictionaryPath);
    assertNotNull(dict.getFST());
  }

  @Deprecated(forRemoval = true, since = "9.1")
  @SuppressWarnings("removal")
  public void testDeprecatedLoadExternalUnknownDictionary() throws Exception {
    String dictionaryPath = UnknownDictionary.class.getName().replace('.', '/');
    UnknownDictionary dict =
        new UnknownDictionary(BinaryDictionary.ResourceScheme.CLASSPATH, dictionaryPath);
    assertNotNull(dict.getCharacterDefinition());
  }

  @Deprecated(forRemoval = true, since = "9.1")
  @SuppressWarnings("removal")
  public void testDeprecatedLoadExternalConnectionCosts() throws Exception {
    String dictionaryPath = ConnectionCosts.class.getName().replace('.', '/');
    ConnectionCosts cc =
        new ConnectionCosts(BinaryDictionary.ResourceScheme.CLASSPATH, dictionaryPath);
    assertEquals(1, cc.get(0, 1));
  }
}
