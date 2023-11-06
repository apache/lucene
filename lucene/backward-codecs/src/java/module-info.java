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

/** Codecs for older versions of Lucene */
module org.apache.lucene.backward_codecs {
  requires org.apache.lucene.core;

  exports org.apache.lucene.backward_codecs;
  exports org.apache.lucene.backward_codecs.compressing;
  exports org.apache.lucene.backward_codecs.lucene40.blocktree;
  exports org.apache.lucene.backward_codecs.lucene50;
  exports org.apache.lucene.backward_codecs.lucene50.compressing;
  exports org.apache.lucene.backward_codecs.lucene60;
  exports org.apache.lucene.backward_codecs.lucene70;
  exports org.apache.lucene.backward_codecs.lucene80;
  exports org.apache.lucene.backward_codecs.lucene84;
  exports org.apache.lucene.backward_codecs.lucene86;
  exports org.apache.lucene.backward_codecs.lucene87;
  exports org.apache.lucene.backward_codecs.lucene90;
  exports org.apache.lucene.backward_codecs.lucene91;
  exports org.apache.lucene.backward_codecs.lucene92;
  exports org.apache.lucene.backward_codecs.lucene94;
  exports org.apache.lucene.backward_codecs.lucene95;
  exports org.apache.lucene.backward_codecs.packed;
  exports org.apache.lucene.backward_codecs.store;

  provides org.apache.lucene.codecs.DocValuesFormat with
      org.apache.lucene.backward_codecs.lucene70.Lucene70DocValuesFormat,
      org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
  provides org.apache.lucene.codecs.PostingsFormat with
      org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat,
      org.apache.lucene.backward_codecs.lucene84.Lucene84PostingsFormat,
      org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsFormat;
  provides org.apache.lucene.codecs.KnnVectorsFormat with
      org.apache.lucene.backward_codecs.lucene90.Lucene90HnswVectorsFormat,
      org.apache.lucene.backward_codecs.lucene91.Lucene91HnswVectorsFormat,
      org.apache.lucene.backward_codecs.lucene92.Lucene92HnswVectorsFormat,
      org.apache.lucene.backward_codecs.lucene94.Lucene94HnswVectorsFormat,
      org.apache.lucene.backward_codecs.lucene95.Lucene95HnswVectorsFormat;
  provides org.apache.lucene.codecs.Codec with
      org.apache.lucene.backward_codecs.lucene70.Lucene70Codec,
      org.apache.lucene.backward_codecs.lucene80.Lucene80Codec,
      org.apache.lucene.backward_codecs.lucene84.Lucene84Codec,
      org.apache.lucene.backward_codecs.lucene86.Lucene86Codec,
      org.apache.lucene.backward_codecs.lucene87.Lucene87Codec,
      org.apache.lucene.backward_codecs.lucene90.Lucene90Codec,
      org.apache.lucene.backward_codecs.lucene91.Lucene91Codec,
      org.apache.lucene.backward_codecs.lucene92.Lucene92Codec,
      org.apache.lucene.backward_codecs.lucene94.Lucene94Codec,
      org.apache.lucene.backward_codecs.lucene95.Lucene95Codec;
}
