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
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;

/** Tests Lucene90DocValuesFormat */
public class TestLucene90DocValuesFormatVariableSkipInterval extends BaseDocValuesFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String name) {
        return new Lucene90DocValuesFormat(random().nextInt(1 << 3, 1 << 10));
      }
    };
  }
}
