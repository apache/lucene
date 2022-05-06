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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * Binary dictionary implementation for a known-word dictionary model: Words are encoded into an FST
 * mapping to a list of wordIDs.
 */
public final class TokenInfoDictionary extends BinaryDictionary {

  public static final String FST_FILENAME_SUFFIX = "$fst.dat";

  private final TokenInfoFST fst;

  private TokenInfoDictionary() throws IOException {
    this(
        () -> getClassResource(TARGETMAP_FILENAME_SUFFIX),
        () -> getClassResource(POSDICT_FILENAME_SUFFIX),
        () -> getClassResource(DICT_FILENAME_SUFFIX),
        () -> getClassResource(FST_FILENAME_SUFFIX));
  }

  /**
   * @param resourceScheme - scheme for loading resources (FILE or CLASSPATH).
   * @param resourcePath - where to load resources (dictionaries) from.
   * @deprecated replaced by {@link #TokenInfoDictionary(Path, Path, Path, Path)} for files and
   *     {@link #TokenInfoDictionary(URL, URL, URL, URL)} for classpath/module resources
   */
  @Deprecated(forRemoval = true, since = "9.1")
  @SuppressWarnings("removal")
  public TokenInfoDictionary(ResourceScheme resourceScheme, String resourcePath)
      throws IOException {
    this(
        () ->
            BinaryDictionary.getResource(resourceScheme, resourcePath + TARGETMAP_FILENAME_SUFFIX),
        () -> BinaryDictionary.getResource(resourceScheme, resourcePath + POSDICT_FILENAME_SUFFIX),
        () -> BinaryDictionary.getResource(resourceScheme, resourcePath + DICT_FILENAME_SUFFIX),
        () -> BinaryDictionary.getResource(resourceScheme, resourcePath + FST_FILENAME_SUFFIX));
  }

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
    this(
        () -> Files.newInputStream(targetMapFile),
        () -> Files.newInputStream(posDictFile),
        () -> Files.newInputStream(dictFile),
        () -> Files.newInputStream(fstFile));
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
    this(
        () -> targetMapUrl.openStream(),
        () -> posDictUrl.openStream(),
        () -> dictUrl.openStream(),
        () -> fstUrl.openStream());
  }

  private TokenInfoDictionary(
      IOSupplier<InputStream> targetMapResource,
      IOSupplier<InputStream> posResource,
      IOSupplier<InputStream> dictResource,
      IOSupplier<InputStream> fstResource)
      throws IOException {
    super(targetMapResource, posResource, dictResource);
    FST<Long> fst;
    try (InputStream is = new BufferedInputStream(fstResource.get())) {
      DataInput in = new InputStreamDataInput(is);
      fst = new FST<>(in, in, PositiveIntOutputs.getSingleton());
    }
    this.fst = new TokenInfoFST(fst);
  }

  private static InputStream getClassResource(String suffix) throws IOException {
    final String resourcePath = TokenInfoDictionary.class.getSimpleName() + suffix;
    return IOUtils.requireResourceNonNull(
        TokenInfoDictionary.class.getResourceAsStream(resourcePath), resourcePath);
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
