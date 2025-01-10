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

import org.apache.lucene.codecs.bitvectors.HnswBitVectorsFormat;

/** Lucene codecs and postings formats */
module org.apache.lucene.codecs {
  requires org.apache.lucene.core;

  exports org.apache.lucene.codecs.bitvectors;
  exports org.apache.lucene.codecs.blockterms;
  exports org.apache.lucene.codecs.blocktreeords;
  exports org.apache.lucene.codecs.bloom;
  exports org.apache.lucene.codecs.memory;
  exports org.apache.lucene.codecs.simpletext;
  exports org.apache.lucene.codecs.uniformsplit;
  exports org.apache.lucene.codecs.uniformsplit.sharedterms;

  provides org.apache.lucene.codecs.KnnVectorsFormat with
      HnswBitVectorsFormat;
  provides org.apache.lucene.codecs.PostingsFormat with
      org.apache.lucene.codecs.blocktreeords.BlockTreeOrdsPostingsFormat,
      org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat,
      org.apache.lucene.codecs.memory.DirectPostingsFormat,
      org.apache.lucene.codecs.memory.FSTPostingsFormat,
      org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat,
      org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat;
  provides org.apache.lucene.codecs.Codec with
      org.apache.lucene.codecs.simpletext.SimpleTextCodec;
}
