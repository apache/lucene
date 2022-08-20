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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;

public class TestSimpleTextKnnVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new SimpleTextCodec();
  }

  public void testUnsupportedEncoding() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("field", newBytesRef(2), VectorSimilarityFunction.DOT_PRODUCT));
      iw.addDocument(doc);
      expectThrows(IllegalArgumentException.class, () -> iw.commit());
    }
  }

  @Override
  public void testRandomBytes() throws Exception {
    // unimplemented
  }

  @Override
  public void testSortedIndexBytes() throws Exception {
    // unimplemented
  }
}
