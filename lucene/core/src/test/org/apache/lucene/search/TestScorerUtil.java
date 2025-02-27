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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;

public class TestScorerUtil extends LuceneTestCase {

  public void testLikelyFixedBits() throws IOException {
    assertNull(ScorerUtil.likelyLiveDocs(null));

    Bits bits1 = new SparseFixedBitSet(10);
    assertNotSame(bits1, ScorerUtil.likelyLiveDocs(bits1));
    Bits bits2 = new Bits.MatchAllBits(10);
    assertNotSame(bits2, ScorerUtil.likelyLiveDocs(bits2));
    assertEquals(
        ScorerUtil.likelyLiveDocs(bits1).getClass(), ScorerUtil.likelyLiveDocs(bits2).getClass());

    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig()
                    .setCodec(TestUtil.getDefaultCodec())
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      Document doc = new Document();
      doc.add(new StringField("id", "1", Store.NO));
      w.addDocument(doc);
      doc = new Document();
      doc.add(new StringField("id", "2", Store.NO));
      w.addDocument(doc);
      w.deleteDocuments(new Term("id", "1"));
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        Bits acceptDocs = leafReader.getLiveDocs();
        assertNotNull(acceptDocs);
        assertSame(acceptDocs, ScorerUtil.likelyLiveDocs(acceptDocs));
      }
    }
  }

  @AwaitsFix(bugUrl = "https://github.com/apache/lucene/issues/14303")
  public void testLikelyImpactsEnum() throws IOException {
    DocIdSetIterator iterator = DocIdSetIterator.all(10);
    assertTrue(ScorerUtil.likelyImpactsEnum(iterator) instanceof FilterDocIdSetIterator);

    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter w =
            new IndexWriter(dir, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
      Document doc = new Document();
      doc.add(new FeatureField("field", "value", 1f));
      w.addDocument(doc);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        TermsEnum te = leafReader.terms("field").iterator();
        assertTrue(te.seekExact(new BytesRef("value")));
        ImpactsEnum ie = te.impacts(PostingsEnum.FREQS);
        assertSame(ie, ScorerUtil.likelyImpactsEnum(ie));
      }
    }
  }
}
