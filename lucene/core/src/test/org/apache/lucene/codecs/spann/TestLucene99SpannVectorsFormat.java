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

public class TestLucene99SpannVectorsFormat extends LuceneTestCase {

    public void testDefaultName() {
        assertEquals("Lucene99SpannVectors", Lucene99SpannVectorsFormat.FORMAT_NAME);
    }

    public void testMaxDimensions() {
        Lucene99SpannVectorsFormat format = new Lucene99SpannVectorsFormat();
        assertEquals(1024, format.getMaxDimensions("any_field"));
    }

    public void testToString() {
        Lucene99SpannVectorsFormat format = new Lucene99SpannVectorsFormat();
        assertEquals("Lucene99SpannVectors", format.getName());
    }

    public void testIntegration() throws Exception {
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
                doc.add(new KnnFloatVectorField("vec", new float[] { 1f, 2f, 3f }));
                writer.addDocument(doc);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.numDocs());
                LeafReader leaf = reader.leaves().get(0).reader();
                leaf.checkIntegrity(); // Forces file header checks
            }
        }
    }
}
