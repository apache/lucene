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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;

/**
 * Updates a doc-values field that none of the segments being merged contains yet, with the update
 * still in flight when the merge runs, and checks the merged index keeps the update.
 */
public class TestMergeDuringUnfinishedDocValuesUpdate extends LuceneTestCase {

  public void testUpdateToFieldNewToSegmentSurvivesConcurrentMerge() throws Exception {
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // Pause the "late" update once it has applied its packet, so the merge runs while it is in
    // flight. BD trace reports each applied packet; "done inner apply del packet" is that point.
    InfoStream pauseLateAfterApply =
        new InfoStream() {
          @Override
          public void message(String component, String message) {
            if (Thread.currentThread().getName().equals("late")
                && message.contains("done inner apply del packet")) {
              held.countDown();
              try {
                release.await(60, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public boolean isEnabled(String component) {
            return component.equals("BD");
          }

          @Override
          public void close() {}
        };
    MergePolicy mergeFirstTwoOnRequest =
        new FilterMergePolicy(NoMergePolicy.INSTANCE) {
          @Override
          public MergeSpecification findMerges(
              MergeTrigger trigger, SegmentInfos infos, MergeContext context) {
            if (trigger != MergeTrigger.EXPLICIT) {
              return null;
            }
            MergeSpecification spec = new MergeSpecification();
            spec.add(new OneMerge(infos.asList().subList(0, 2)));
            return spec;
          }
        };
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(TestUtil.getDefaultCodec())
            .setMergePolicy(mergeFirstTwoOnRequest)
            .setMergeScheduler(new SerialMergeScheduler())
            .setInfoStream(pauseLateAfterApply);
    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      // Two segments to merge. "tag" is absent from both.
      Document a = new Document();
      a.add(new StringField("id", "a", Field.Store.NO));
      a.add(new NumericDocValuesField("num", 0));
      w.addDocument(a);
      w.flush();
      w.addDocument(new Document());
      w.flush();

      w.updateNumericDocValue(new Term("id", "a"), "num", 7);
      Document c = new Document();
      c.add(new BinaryDocValuesField("tag", new BytesRef("c")));
      w.addDocument(c);
      w.flushNextBuffer();

      // Give "a" a "tag" value: a field new to the two segments being merged.
      FutureTask<Void> lateUpdate =
          new FutureTask<>(
              () -> {
                w.updateBinaryDocValue(new Term("id", "a"), "tag", new BytesRef("a"));
                w.addDocument(new Document());
                w.flushNextBuffer();
                return null;
              });
      Thread late = new Thread(lateUpdate, "late");
      late.start();
      assertTrue(held.await(60, TimeUnit.SECONDS));
      try {
        w.maybeMerge();
      } finally {
        release.countDown();
        late.join();
      }
      lateUpdate.get();

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertEquals(4, reader.numDocs());
        TopDocs hits = new IndexSearcher(reader).search(new TermQuery(new Term("id", "a")), 1);
        assertEquals(1, hits.totalHits.value());
        int docID = hits.scoreDocs[0].doc;
        LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(docID, reader.leaves()));
        int segDoc = docID - leaf.docBase;
        NumericDocValues num = leaf.reader().getNumericDocValues("num");
        assertTrue(num.advanceExact(segDoc));
        assertEquals(7, num.longValue());
        BinaryDocValues tag = leaf.reader().getBinaryDocValues("tag");
        assertTrue(tag.advanceExact(segDoc));
        assertEquals(new BytesRef("a"), tag.binaryValue());
      }
    }
  }
}
