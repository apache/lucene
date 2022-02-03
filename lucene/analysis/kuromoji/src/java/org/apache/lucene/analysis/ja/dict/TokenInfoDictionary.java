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
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
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

  /**
   * @param resourceScheme - scheme for loading resources (FILE or CLASSPATH).
   * @param resourcePath - where to load resources (dictionaries) from. If null, with CLASSPATH
   *     scheme only, use this class's name as the path.
   * @deprecated replaced by {@link #TokenInfoDictionary(String)}
   */
  @Deprecated
  public TokenInfoDictionary(ResourceScheme resourceScheme, String resourcePath)
      throws IOException {
    super(resourceScheme, resourcePath);
    FST<Long> fst;
    try (InputStream is = new BufferedInputStream(getResource(FST_FILENAME_SUFFIX))) {
      DataInput in = new InputStreamDataInput(is);
      fst = new FST<>(in, in, PositiveIntOutputs.getSingleton());
    }
    // TODO: some way to configure?
    this.fst = new TokenInfoFST(fst, true);
  }

  /**
   * Create a {@link TokenInfoDictionary} from an external resource path.
   *
   * @param resourceLocation where to load resources (dictionaries) from.
   * @throws IOException
   */
  public TokenInfoDictionary(String resourceLocation) throws IOException {
    this(
        openFileOrThrowRuntimeException(Paths.get(resourceLocation + TARGETMAP_FILENAME_SUFFIX)),
        openFileOrThrowRuntimeException(Paths.get(resourceLocation + POSDICT_FILENAME_SUFFIX)),
        openFileOrThrowRuntimeException(Paths.get(resourceLocation + DICT_FILENAME_SUFFIX)));
  }

  private TokenInfoDictionary() throws IOException {
    this(
        getClassResourceOrThrowRuntimeException(TARGETMAP_FILENAME_SUFFIX),
        getClassResourceOrThrowRuntimeException(POSDICT_FILENAME_SUFFIX),
        getClassResourceOrThrowRuntimeException(DICT_FILENAME_SUFFIX));
  }

  private TokenInfoDictionary(
      Supplier<InputStream> targetMapResource,
      Supplier<InputStream> posResource,
      Supplier<InputStream> dictResource)
      throws IOException {
    super(targetMapResource, posResource, dictResource);
    FST<Long> fst;
    try (InputStream is =
        new BufferedInputStream(
            getClassResourceOrThrowRuntimeException(FST_FILENAME_SUFFIX).get())) {
      DataInput in = new InputStreamDataInput(is);
      fst = new FST<>(in, in, PositiveIntOutputs.getSingleton());
    }
    // TODO: some way to configure?
    this.fst = new TokenInfoFST(fst, true);
  }

  private static Supplier<InputStream> getClassResourceOrThrowRuntimeException(String suffix)
      throws RuntimeException {
    final String resourcePath = TokenInfoDictionary.class.getSimpleName() + suffix;
    return () -> {
      try {
        return IOUtils.requireResourceNonNull(
            TokenInfoDictionary.class.getResourceAsStream(resourcePath), resourcePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
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
