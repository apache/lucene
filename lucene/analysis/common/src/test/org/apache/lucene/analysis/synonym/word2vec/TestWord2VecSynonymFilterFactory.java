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

import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.util.ClasspathResourceLoader;
import org.apache.lucene.util.ResourceLoader;
import org.junit.Test;

public class TestWord2VecSynonymFilterFactory extends BaseTokenStreamFactoryTestCase {

  public static final String FACTORY_NAME = "Word2VecSynonym";
  private static final String WORD2VEC_MODEL_FILE = "word2vec-model.zip";

  @Test
  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    Word2VecSynonymFilterFactory factory =
        (Word2VecSynonymFilterFactory)
            tokenFilterFactory(
                FACTORY_NAME, "model", WORD2VEC_MODEL_FILE, "minAcceptedSimilarity", "0.7");

    Word2VecSynonymProvider synonymProvider = factory.getSynonymProvider();
    assertNotEquals(null, synonymProvider);
  }

  @Test
  public void missingRequiredArgument_shouldThrowException() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME,
                  "format",
                  "dl4j",
                  "minAcceptedSimilarity",
                  "0.7",
                  "maxSynonymsPerTerm",
                  "10");
            });
    assertTrue(expected.getMessage().contains("Configuration Error: missing parameter 'model'"));
  }

  @Test
  public void unsupportedModelFormat_shouldThrowException() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME, "model", WORD2VEC_MODEL_FILE, "format", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Model format 'BOGUSVALUE' not supported"));
  }

  @Test
  public void bogusArgument_shouldThrowException() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME, "model", WORD2VEC_MODEL_FILE, "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  @Test
  public void illegalArguments_shouldThrowException() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME,
                  "model",
                  WORD2VEC_MODEL_FILE,
                  "minAcceptedSimilarity",
                  "2",
                  "maxSynonymsPerTerm",
                  "10");
            });
    assertTrue(
        expected
            .getMessage()
            .contains("minAcceptedSimilarity must be in the range (0, 1]. Found: 2"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME,
                  "model",
                  WORD2VEC_MODEL_FILE,
                  "minAcceptedSimilarity",
                  "0",
                  "maxSynonymsPerTerm",
                  "10");
            });
    assertTrue(
        expected
            .getMessage()
            .contains("minAcceptedSimilarity must be in the range (0, 1]. Found: 0"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME,
                  "model",
                  WORD2VEC_MODEL_FILE,
                  "minAcceptedSimilarity",
                  "0.7",
                  "maxSynonymsPerTerm",
                  "-1");
            });
    assertTrue(
        expected
            .getMessage()
            .contains("maxSynonymsPerTerm must be a positive integer greater than 0. Found: -1"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  FACTORY_NAME,
                  "model",
                  WORD2VEC_MODEL_FILE,
                  "minAcceptedSimilarity",
                  "0.7",
                  "maxSynonymsPerTerm",
                  "0");
            });
    assertTrue(
        expected
            .getMessage()
            .contains("maxSynonymsPerTerm must be a positive integer greater than 0. Found: 0"));
  }
}
