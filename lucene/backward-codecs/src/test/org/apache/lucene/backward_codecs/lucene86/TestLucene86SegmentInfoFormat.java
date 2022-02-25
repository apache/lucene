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

package org.apache.lucene.backward_codecs.lucene86;

import org.apache.lucene.backward_codecs.lucene87.Lucene87RWCodec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseSegmentInfoFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.Version;

@Nightly // N-2 formats are only tested on nightly runs
public class TestLucene86SegmentInfoFormat extends BaseSegmentInfoFormatTestCase {

  @Override
  protected Version[] getVersions() {
    return new Version[] {Version.fromBits(8, 8, 1)};
  }

  @Override
  protected Codec getCodec() {
    return new Lucene87RWCodec();
  }
}
