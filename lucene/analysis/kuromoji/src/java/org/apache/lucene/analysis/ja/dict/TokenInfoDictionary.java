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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.analysis.morph.BinaryDictionary;
import org.apache.lucene.analysis.morph.MorphFSTLoader;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

/**
 * Binary dictionary implementation for a known-word dictionary model: Words are encoded into an FST
 * mapping to a list of wordIDs.
 */
public final class TokenInfoDictionary extends BinaryDictionary<TokenInfoMorphData> {

  public static final String FST_FILENAME_SUFFIX = "$fst.dat";

  private final TokenInfoFST fst;
  private final TokenInfoMorphData morphAtts;
  /** Keeps mmap resources alive for {@link MorphFSTLoader#loadFromPath}; otherwise null. */
  @SuppressWarnings("unused")
  private final MorphFSTLoader.LoadedFST fstHolder;

  /**
   * Create a {@link TokenInfoDictionary} from an external resource path.
   *
   * @param targetMapFile where to load target map resource
   * @param posDictFile where to load POS dictionary resource
   * @param dictFile where to load dictionary entries resource
   * @param fstFile where to load encoded FST data resource
   * @throws IOException if resource was not found or broken
   */
  public TokenInfoDictionary(Path targetMapFile, Path posDictFile, Path dictFile, Path fstFile)
      throws IOException {
    super(
        () -> Files.newInputStream(targetMapFile),
        () -> Files.newInputStream(dictFile),
        DictionaryConstants.TARGETMAP_HEADER,
        DictionaryConstants.DICT_HEADER,
        DictionaryConstants.VERSION);
    this.morphAtts =
        new TokenInfoMorphData(buffer, () -> Files.newInputStream(posDictFile));
    MorphFSTLoader.LoadedFST loaded = MorphFSTLoader.loadFromPath(fstFile);
    this.fstHolder = loaded;
    this.fst = new TokenInfoFST(loaded.fst(), true);
  }

  /**
   * Create a {@link TokenInfoDictionary} from an external resource URL (e.g. from Classpath with
   * {@link ClassLoader#getResource(String)}).
   *
   * @param targetMapUrl where to load target map resource
   * @param posDictUrl where to load POS dictionary resource
   * @param dictUrl where to load dictionary entries resource
   * @param fstUrl where to load encoded FST data resource
   * @throws IOException if resource was not found or broken
   */
  public TokenInfoDictionary(URL targetMapUrl, URL posDictUrl, URL dictUrl, URL fstUrl)
      throws IOException {
    super(
        () -> targetMapUrl.openStream(),
        () -> dictUrl.openStream(),
        DictionaryConstants.TARGETMAP_HEADER,
        DictionaryConstants.DICT_HEADER,
        DictionaryConstants.VERSION);
    this.morphAtts = new TokenInfoMorphData(buffer, () -> posDictUrl.openStream());
    MorphFSTLoader.LoadedFST loaded = MorphFSTLoader.loadFromUrl(fstUrl);
    this.fstHolder = loaded.resource() != null ? loaded : null;
    this.fst = new TokenInfoFST(loaded.fst(), true);
  }

  private TokenInfoDictionary() throws IOException {
    this(
        () -> getClassResource(TARGETMAP_FILENAME_SUFFIX),
        () -> getClassResource(POSDICT_FILENAME_SUFFIX),
        () -> getClassResource(DICT_FILENAME_SUFFIX),
        () -> getClassResource(FST_FILENAME_SUFFIX));
  }

  private TokenInfoDictionary(
      IOSupplier<InputStream> targetMapResource,
      IOSupplier<InputStream> posResource,
      IOSupplier<InputStream> dictResource,
      IOSupplier<InputStream> fstResource)
      throws IOException {
    super(
        targetMapResource,
        dictResource,
        DictionaryConstants.TARGETMAP_HEADER,
        DictionaryConstants.DICT_HEADER,
        DictionaryConstants.VERSION);
    this.morphAtts = new TokenInfoMorphData(buffer, posResource);
    try (InputStream is = new BufferedInputStream(fstResource.get())) {
      // TODO: some way to configure?
      this.fst = new TokenInfoFST(MorphFSTLoader.loadFromStream(is), true);
    }
    this.fstHolder = null;
  }

  static InputStream getClassResource(String suffix) throws IOException {
    final String resourcePath = TokenInfoDictionary.class.getSimpleName() + suffix;
    return IOUtils.requireResourceNonNull(
        TokenInfoDictionary.class.getResourceAsStream(resourcePath), resourcePath);
  }

  @Override
  public TokenInfoMorphData getMorphAttributes() {
    return morphAtts;
  }

  public TokenInfoFST getFST() {
    return fst;
  }

  public static TokenInfoDictionary getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private static class SingletonHolder {
    static final TokenInfoDictionary INSTANCE;

    static {
      try {
        INSTANCE = new TokenInfoDictionary();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load TokenInfoDictionary.", ioe);
      }
    }
  }
}
