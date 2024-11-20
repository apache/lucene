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

/** Lucene test framework. */
@SuppressWarnings({"module", "requires-automatic", "requires-transitive-automatic"})
module org.apache.lucene.test_framework {
  requires org.apache.lucene.core;
  requires org.apache.lucene.codecs;
  requires transitive junit;
  requires transitive randomizedtesting.runner;

  // Open certain packages for junit because it scans methods via reflection.
  opens org.apache.lucene.tests.index to
      junit;

  exports org.apache.lucene.tests.analysis.standard;
  exports org.apache.lucene.tests.analysis;
  exports org.apache.lucene.tests.codecs.asserting;
  exports org.apache.lucene.tests.codecs.blockterms;
  exports org.apache.lucene.tests.codecs.bloom;
  exports org.apache.lucene.tests.codecs.cheapbastard;
  exports org.apache.lucene.tests.codecs.compressing.dummy;
  exports org.apache.lucene.tests.codecs.compressing;
  exports org.apache.lucene.tests.codecs.cranky;
  exports org.apache.lucene.tests.codecs.mockrandom;
  exports org.apache.lucene.tests.codecs.ramonly;
  exports org.apache.lucene.tests.codecs.uniformsplit.sharedterms;
  exports org.apache.lucene.tests.codecs.uniformsplit;
  exports org.apache.lucene.tests.codecs.vector;
  exports org.apache.lucene.tests.geo;
  exports org.apache.lucene.tests.index;
  exports org.apache.lucene.tests.mockfile;
  exports org.apache.lucene.tests.search.similarities;
  exports org.apache.lucene.tests.search;
  exports org.apache.lucene.tests.store;
  exports org.apache.lucene.tests.util.automaton;
  exports org.apache.lucene.tests.util.fst;
  exports org.apache.lucene.tests.util;

  provides org.apache.lucene.codecs.Codec with
      org.apache.lucene.tests.codecs.asserting.AssertingCodec,
      org.apache.lucene.tests.codecs.cheapbastard.CheapBastardCodec,
      org.apache.lucene.tests.codecs.compressing.DeflateWithPresetCompressingCodec,
      org.apache.lucene.tests.codecs.compressing.FastCompressingCodec,
      org.apache.lucene.tests.codecs.compressing.FastDecompressionCompressingCodec,
      org.apache.lucene.tests.codecs.compressing.HighCompressionCompressingCodec,
      org.apache.lucene.tests.codecs.compressing.LZ4WithPresetCompressingCodec,
      org.apache.lucene.tests.codecs.compressing.dummy.DummyCompressingCodec,
      org.apache.lucene.tests.codecs.vector.ConfigurableMCodec;
  provides org.apache.lucene.codecs.DocValuesFormat with
      org.apache.lucene.tests.codecs.asserting.AssertingDocValuesFormat;
  provides org.apache.lucene.codecs.KnnVectorsFormat with
      org.apache.lucene.tests.codecs.asserting.AssertingKnnVectorsFormat;
  provides org.apache.lucene.codecs.PostingsFormat with
      org.apache.lucene.tests.codecs.mockrandom.MockRandomPostingsFormat,
      org.apache.lucene.tests.codecs.ramonly.RAMOnlyPostingsFormat,
      org.apache.lucene.tests.codecs.blockterms.LuceneFixedGap,
      org.apache.lucene.tests.codecs.blockterms.LuceneVarGapFixedInterval,
      org.apache.lucene.tests.codecs.blockterms.LuceneVarGapDocFreqInterval,
      org.apache.lucene.tests.codecs.bloom.TestBloomFilteredLucenePostings,
      org.apache.lucene.tests.codecs.asserting.AssertingPostingsFormat,
      org.apache.lucene.tests.codecs.uniformsplit.UniformSplitRot13PostingsFormat,
      org.apache.lucene.tests.codecs.uniformsplit.sharedterms.STUniformSplitRot13PostingsFormat;
}
