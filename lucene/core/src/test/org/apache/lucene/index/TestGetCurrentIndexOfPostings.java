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
package org.apache.lucene.index;

import org.apache.lucene.codecs.lucene91.Lucene91Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

// TODO: we really need to test indexingoffsets, but then getting only docs / docs + freqs.
// not all codecs store prx separate...
// TODO: fix sep codec to index offsets so we can greatly reduce this list!
public class TestGetCurrentIndexOfPostings extends LuceneTestCase {

    public void testGetIndexOfPostings() throws Exception {
        String sortField = "sort_field";
        String textField = "text_field";
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.codec = new Lucene91Codec();
        // use index sort to keep docId stable
        Sort indexSort = new Sort(new SortedNumericSortField(sortField, SortField.Type.LONG, false));
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < 10; j++) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField(sortField, i * 10 + j));
                doc.add(newTextField(textField, "a" + j, Field.Store.NO));
                writer.addDocument(doc);
            }
        }
        writer.flush();
        writer.forceMerge(1);
        DirectoryReader reader = writer.getReader();
        writer.close();
        LeafReader sub = getOnlyLeafReader(reader);
        // for term "a0" docId list is: 0 10 20 30 40 50 ... 4990
        Terms terms = sub.terms(textField);
        TermsEnum termsEnum = terms.iterator();
        termsEnum.seekExact(new BytesRef("a0".getBytes()));
        PostingsEnum postingsEnum = termsEnum.postings(null);
        int docId = 0;
        while (true) {
            int currentDocId = postingsEnum.nextDoc();
            if (currentDocId == NO_MORE_DOCS) {
                break;
            } else {
                Assert.assertEquals(docId, currentDocId);
                Assert.assertEquals(docId / 10, postingsEnum.getCurrentIndexOfPostings());
                docId += 10;
            }
        }
        postingsEnum = termsEnum.postings(null);
        System.out.println(postingsEnum.getClass().toString());
        docId = postingsEnum.advance(0);
        Assert.assertEquals(0, docId);
        Assert.assertEquals(0, postingsEnum.getCurrentIndexOfPostings());
        docId = postingsEnum.advance(18);
        Assert.assertEquals(20, docId);
        Assert.assertEquals(2, postingsEnum.getCurrentIndexOfPostings());

        docId = postingsEnum.advance(3999);
        Assert.assertEquals(4000, docId);
        Assert.assertEquals(400, postingsEnum.getCurrentIndexOfPostings());

        docId = postingsEnum.advance(4990);
        Assert.assertEquals(4990, docId);
        Assert.assertEquals(499, postingsEnum.getCurrentIndexOfPostings());

        docId = postingsEnum.advance(499000);
        Assert.assertEquals(NO_MORE_DOCS, docId);
        Assert.assertEquals(500, postingsEnum.getCurrentIndexOfPostings());

        reader.close();
        dir.close();
    }

}
