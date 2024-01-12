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
package org.apache.lucene.codecs.simpletext;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** Tests SimpleTextDocValuesFormat */
public class TestSimpleTextDocValuesFormat extends BaseDocValuesFormatTestCase {
  private final Codec codec = new SimpleTextCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testFileIsUTF8() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
      try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf)) {
        for (int i = 0; i < 100; i++) {
          writer.addDocument(
              List.of(
                  new SortedDocValuesField(
                      "sortedVal", newBytesRef(TestUtil.randomSimpleString(random()))),
                  new SortedSetDocValuesField(
                      "sortedSetVal", newBytesRef(TestUtil.randomSimpleString(random()))),
                  new NumericDocValuesField("numberVal", random().nextLong()),
                  new BinaryDocValuesField("binaryVal", TestUtil.randomBinaryTerm(random()))));
        }
      }
      for (String file : dir.listAll()) {
        if (file.endsWith("dat")) {
          try (IndexInput input = dir.openChecksumInput(file, IOContext.READONCE)) {
            long length = input.length();
            if (length > 20_000) {
              // Avoid allocating a huge array if the length is wrong
              fail("Doc values should not be this large");
            }
            byte[] bytes = new byte[(int) length];
            input.readBytes(bytes, 0, bytes.length);
            BytesRef bytesRef = new BytesRef(bytes);
            assertNotEquals(bytesRef.toString(), Term.toString(bytesRef));
          }
        }
      }
    }
  }
}
