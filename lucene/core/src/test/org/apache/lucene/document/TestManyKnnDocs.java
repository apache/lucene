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
package org.apache.lucene.document;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.codecs.vector.ConfigurableMCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Monster;
import org.apache.lucene.tests.util.TestUtil;

/** Tests many KNN docs to look for overflows. */
@TimeoutSuite(millis = 86_400_000) // 24 hour timeout
@Monster("takes ~??? hours and needs extra heap, disk space, file handles")
public class TestManyKnnDocs extends LuceneTestCase {
    // ./gradlew -p lucene/core test --tests TestManyKnnDocs -Dtests.verbose=true -Dtests.monster=true
    //  -Ptests.heapsize=16g -Ptests.jvmargs=-XX:ActiveProcessorCount=1
    public void testLargeSegment() throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(new ConfigurableMCodec(128)); // Make sure to use our custom codec instead of a random one
        iwc.setRAMBufferSizeMB(64); // Use a 64MB buffer to create larger initial segments
        TieredMergePolicy mp = new TieredMergePolicy();
        mp.setMaxMergeAtOnce(256); // avoid intermediate merges (waste of time with HNSW?)
        mp.setSegmentsPerTier(256); // only merge once at the end when we ask
        iwc.setMergePolicy(mp);
        iwc.setInfoStream(System.out);

        String fieldName = "field";
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;

        try (Directory dir = FSDirectory.open(createTempDir("ManyKnnVectorDocs"));
             IndexWriter iw = new IndexWriter(dir, iwc)) {

            // This data is enough to trigger the overflow bug in issue #11905... maybe?
            int numVectors = 2500000;

            float[] vector = new float[1];
            Document doc = new Document();
            doc.add(new KnnVectorField(fieldName, vector, similarityFunction));

            for (int i = 0; i < numVectors; i++) {
                // kinda strange that there's no defensive copy and this works without even setVectorValue
                // but whatever
                vector[0] = (float) i;
                iw.addDocument(doc);
                if (VERBOSE && i % 10_000 == 0) {
                    System.out.println("Indexed " + i + " vectors out of " + numVectors);
                }
            }

            // merge to single segment and then verify
            iw.forceMerge(1);
            iw.commit();
            TestUtil.checkIndex(dir);

            IndexReader ir = DirectoryReader.open(iw);
            IndexSearcher searcher = new IndexSearcher(ir);
            KnnVectorQuery query = new KnnVectorQuery(fieldName, new float[] {0}, 10);
            searcher.search(query, 10);
        }
    }
}
