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
package org.apache.lucene.sandbox.search;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.sandbox.document.LongPointMultiRangeBuilder;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

public class TestSsDvMultiRangeQuery extends LuceneTestCase  {
    public void testDuelWithStandardDisjunction() throws IOException {
        int iterations = LuceneTestCase.TEST_NIGHTLY ? atLeast(100) : 10;
        for (int iter = 0; iter < iterations; iter++) {
            Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);

            int dims = 1;
            boolean singleton = rarely();
            long[] scratch = new long[dims];
            for (int i = 0; i < 100; i++) {
                int numPoints = singleton ? 1 : RandomNumbers.randomIntBetween(random(), 1, 10);
                Document doc = new Document();
                for (int j = 0; j < numPoints; j++) {
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = RandomNumbers.randomLongBetween(random(), 0, 100);
                    }
                    doc.add(new LongPoint("point", scratch));
                    if (singleton) {
                        doc.add(new SortedDocValuesField("docVal", LongPoint.pack(scratch)));
                    } else {
                        doc.add(new SortedSetDocValuesField("docVal", LongPoint.pack(scratch)));
                    }
                }
                w.addDocument(doc);
            }

            IndexReader reader = w.getReader();
            IndexSearcher searcher = newSearcher(reader);

            int numRanges = RandomNumbers.randomIntBetween(random(), 1, 20);
            LongPointMultiRangeBuilder builder1 = new LongPointMultiRangeBuilder("point", dims);
            BooleanQuery.Builder builder2 = new BooleanQuery.Builder();
            SortedSetDocValuesMultiRangeQuery.FieldValueFixedBuilder builder3 = new SortedSetDocValuesMultiRangeQuery.FieldValueFixedBuilder("docVal", Long.BYTES);

            for (int i = 0; i < numRanges; i++) {
                long[] lower = new long[dims];
                long[] upper = new long[dims];
                for (int j = 0; j < dims; j++) {
                    lower[j] = RandomNumbers.randomLongBetween(random(), -100, 200);
                    upper[j] = lower[j] + RandomNumbers.randomLongBetween(random(), 0, 100);
                }
                builder1.add(lower, upper);
                builder2.add(LongPoint.newRangeQuery("point", lower, upper), BooleanClause.Occur.SHOULD);
                builder3.add(LongPoint.pack(lower), LongPoint.pack(upper));
            }

            Query query1 = builder1.build();
            Query query2 = builder2.build();
            Query query3 = builder3.build();
            TopDocs result1 = searcher.search(query1, 10, Sort.INDEXORDER);
            TopDocs result2 = searcher.search(query2, 10, Sort.INDEXORDER);
            TopDocs result3 = searcher.search(query3, 10, Sort.INDEXORDER);
            assertEquals(result2.totalHits, result1.totalHits);
            assertEquals(result2.totalHits, result3.totalHits);
            assertEquals(result2.scoreDocs.length, result1.scoreDocs.length);
            assertEquals(result2.scoreDocs.length, result3.scoreDocs.length);
            for (int i = 0; i < result2.scoreDocs.length; i++) {
                assertEquals(result2.scoreDocs[i].doc, result1.scoreDocs[i].doc);
                assertEquals(result3.scoreDocs[i].doc, result3.scoreDocs[i].doc);
            }

            IOUtils.close(reader, w, dir);
        }
    }

}
