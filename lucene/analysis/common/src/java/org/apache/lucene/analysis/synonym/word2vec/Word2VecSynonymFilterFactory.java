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

package org.apache.lucene.analysis.synonym.word2vec;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.word2vec.Word2VecSynonymProviderFactory.Word2VecSupportedFormats;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link Word2VecSynonymFilter}.
 *
 * @lucene.experimental
 * @lucene.spi {@value #NAME}
 */
public class Word2VecSynonymFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "Word2VecSynonym";

  public static final int DEFAULT_MAX_SYNONYMS_PER_TERM = 5;
  public static final float DEFAULT_MIN_ACCEPTED_SIMILARITY = 0.8f;

  private final int maxSynonymsPerTerm;
  private final float minAcceptedSimilarity;
  private final Word2VecSupportedFormats format;
  private final String word2vecModelFileName;

  private Word2VecSynonymProvider synonymProvider;

  public Word2VecSynonymFilterFactory(Map<String, String> args) {
    super(args);
    this.maxSynonymsPerTerm = getInt(args, "maxSynonymsPerTerm", DEFAULT_MAX_SYNONYMS_PER_TERM);
    this.minAcceptedSimilarity =
        getFloat(args, "minAcceptedSimilarity", DEFAULT_MIN_ACCEPTED_SIMILARITY);
    this.word2vecModelFileName = require(args, "model");

    String modelFormat = get(args, "format", "dl4j").toUpperCase(Locale.ROOT);
    try {
      this.format = Word2VecSupportedFormats.valueOf(modelFormat);
    } catch (IllegalArgumentException exc) {
      throw new IllegalArgumentException("Model format '" + modelFormat + "' not supported", exc);
    }

    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
    if (minAcceptedSimilarity <= 0 || minAcceptedSimilarity > 1) {
      throw new IllegalArgumentException(
          "minAcceptedSimilarity must be in the range (0, 1]. Found: " + minAcceptedSimilarity);
    }
    if (maxSynonymsPerTerm <= 0) {
      throw new IllegalArgumentException(
          "maxSynonymsPerTerm must be a positive integer greater than 0. Found: "
              + maxSynonymsPerTerm);
    }
  }

  /** Default ctor for compatibility with SPI */
  public Word2VecSynonymFilterFactory() {
    throw defaultCtorException();
  }

  Word2VecSynonymProvider getSynonymProvider() {
    return this.synonymProvider;
  }

  @Override
  public TokenStream create(TokenStream input) {
    return synonymProvider == null
        ? input
        : new Word2VecSynonymFilter(
            input, synonymProvider, maxSynonymsPerTerm, minAcceptedSimilarity);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    this.synonymProvider =
        Word2VecSynonymProviderFactory.getSynonymProvider(loader, word2vecModelFileName, format);
  }
}
