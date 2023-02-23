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
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.util.ResourceLoader;

/**
 * Supply Word2Vec SynonymProvider cache avoiding that multiple instances of
 * Word2VecSynonymFilterFactory will instantiate multiple instances of the same SynonymProvider.
 * Assumes synonymProvider implementations are thread-safe.
 */
public class Word2VecSynonymProviderFactory {

  enum Word2VecSupportedFormats {
    DL4J
  }

  private static Map<String, SynonymProvider> word2vecSynonymProviders = new ConcurrentHashMap<>();

  public static SynonymProvider getSynonymProvider(
      ResourceLoader loader, String modelFileName, Word2VecSupportedFormats format)
      throws IOException {
    SynonymProvider synonymProvider = word2vecSynonymProviders.get(modelFileName);
    if (synonymProvider == null) {
      try (InputStream stream = loader.openResource(modelFileName)) {
        try (Word2VecModelReader reader = getModelReader(modelFileName, format, stream)) {
          synonymProvider = new Word2VecSynonymProvider(reader.read());
        }
      }
      word2vecSynonymProviders.put(modelFileName, synonymProvider);
    }
    return synonymProvider;
  }

  private static Word2VecModelReader getModelReader(
      String modelFileName, Word2VecSupportedFormats format, InputStream stream) {
    switch (format) {
      case DL4J:
        return new Dl4jModelReader(modelFileName, stream);
    }
    return null;
  }
}
