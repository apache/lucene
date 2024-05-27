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

import org.apache.lucene.codecs.lucene99.Lucene99Codec;

/** Lucene Core. */
@SuppressWarnings("module") // the test framework is compiled after the core...
module org.apache.lucene.core {
  requires java.logging;
  requires static jdk.unsupported; // this is optional but without it MMapDirectory won't be enabled
  requires static jdk.management; // this is optional but explicit declaration is recommended

  exports org.apache.lucene.analysis.standard;
  exports org.apache.lucene.analysis.tokenattributes;
  exports org.apache.lucene.analysis;
  exports org.apache.lucene.codecs.compressing;
  exports org.apache.lucene.codecs.lucene90.blocktree;
  exports org.apache.lucene.codecs.lucene90.compressing;
  exports org.apache.lucene.codecs.lucene90;
  exports org.apache.lucene.codecs.lucene94;
  exports org.apache.lucene.codecs.lucene95;
  exports org.apache.lucene.codecs.lucene99;
  exports org.apache.lucene.codecs.perfield;
  exports org.apache.lucene.codecs;
  exports org.apache.lucene.document;
  exports org.apache.lucene.geo;
  exports org.apache.lucene.index;
  exports org.apache.lucene.search.comparators;
  exports org.apache.lucene.search.knn;
  exports org.apache.lucene.search.similarities;
  exports org.apache.lucene.search;
  exports org.apache.lucene.store;
  exports org.apache.lucene.util.automaton;
  exports org.apache.lucene.util.bkd;
  exports org.apache.lucene.util.compress;
  exports org.apache.lucene.util.fst;
  exports org.apache.lucene.util.graph;
  exports org.apache.lucene.util.hnsw;
  exports org.apache.lucene.util.mutable;
  exports org.apache.lucene.util.packed;
  exports org.apache.lucene.util;

  // Temporarily export HPPC to all modules (eventually, this
  // should be restricted to only Lucene modules)
  exports org.apache.lucene.internal.hppc;

  // Only export internal packages to the test framework.
  exports org.apache.lucene.internal.tests to
      org.apache.lucene.test_framework;

  // Open certain packages for the test framework (ram usage tester).
  opens org.apache.lucene.document to
      org.apache.lucene.test_framework;

  exports org.apache.lucene.util.quantization;
  exports org.apache.lucene.codecs.hnsw;

  provides org.apache.lucene.analysis.TokenizerFactory with
      org.apache.lucene.analysis.standard.StandardTokenizerFactory;
  provides org.apache.lucene.codecs.Codec with
      Lucene99Codec;
  provides org.apache.lucene.codecs.DocValuesFormat with
      org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
  provides org.apache.lucene.codecs.KnnVectorsFormat with
      org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat,
      org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
  provides org.apache.lucene.codecs.PostingsFormat with
      org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat;
  provides org.apache.lucene.index.SortFieldProvider with
      org.apache.lucene.search.SortField.Provider,
      org.apache.lucene.search.SortedNumericSortField.Provider,
      org.apache.lucene.search.SortedSetSortField.Provider;

  uses org.apache.lucene.analysis.CharFilterFactory;
  uses org.apache.lucene.analysis.TokenFilterFactory;
  uses org.apache.lucene.analysis.TokenizerFactory;
  uses org.apache.lucene.codecs.Codec;
  uses org.apache.lucene.codecs.DocValuesFormat;
  uses org.apache.lucene.codecs.KnnVectorsFormat;
  uses org.apache.lucene.codecs.PostingsFormat;
  uses org.apache.lucene.index.SortFieldProvider;
}
