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
package org.apache.lucene.codecs.spann;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestLucene99SpannVectorsReader extends LuceneTestCase {

  public void testCheckIntegrity() throws Exception {
    try (Directory dir = newDirectory()) {
      Codec codec = new Lucene104Codec() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return new Lucene99SpannVectorsFormat();
        }
      };

      IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("vec", new float[] { 1, 2, 3 }));
        writer.addDocument(doc);
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        leaf.checkIntegrity();
      }
    }
  }
}
