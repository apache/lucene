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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;

public class TestMergeDocValuesCarryOverRace extends LuceneTestCase {

  public void testLostUpdate() throws Exception {
    CountDownLatch mergeWarming = new CountDownLatch(1);
    CountDownLatch releaseMerge = new CountDownLatch(1);
    CountDownLatch packetApplied = new CountDownLatch(1);
    CountDownLatch releasePacket = new CountDownLatch(1);
    AtomicReference<Thread> pauseThread = new AtomicReference<>();
    AtomicReference<MergeScheduler.MergeSource> source = new AtomicReference<>();
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(TestUtil.getDefaultCodec())
            // The bug lives on the on-disk carry-over path, which runs only when maxDocValuesOverlays > 0.
            .setMaxDocValuesOverlays(16)
            .setMergeScheduler(
                new MergeScheduler() {
                  @Override
                  public void merge(MergeSource mergeSource, MergeTrigger trigger) {
                    source.set(mergeSource);
                  }

                  @Override
                  public void close() {}
                })
            .setMergedSegmentWarmer(
                reader -> {
                  mergeWarming.countDown();
                  await(releaseMerge);
                })
            .setInfoStream(
                new InfoStream() {
                  @Override
                  public void message(String component, String message) {
                    if (message.startsWith("applyDocValuesUpdates")
                        && pauseThread.compareAndSet(Thread.currentThread(), null)) {
                      packetApplied.countDown();
                      await(releasePacket);
                    }
                  }

                  @Override
                  public boolean isEnabled(String component) {
                    return "BD".equals(component);
                  }

                  @Override
                  public void close() {}
                });
    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      w.addDocument(doc("k0"));
      w.addDocument(doc("k1"));
      w.flush();
      w.addDocument(doc("k1"));
      w.flush();
      w.forceMerge(1, false);
      MergePolicy.OneMerge merge = source.get().getNextMerge();
      Thread merger = new Thread(() -> run(() -> source.get().merge(merge)));
      merger.start();
      await(mergeWarming);

      w.getConfig().setRAMBufferSizeMB(0.00001);
      w.updateNumericDocValue(new Term("key", "k0"), "n", 1);
      DirectoryReader.open(w).close();
      Thread updater =
          new Thread(() -> run(() -> w.updateNumericDocValue(new Term("key", "k0"), "n", 2)));
      pauseThread.set(updater);
      updater.start();
      await(packetApplied);
      w.updateNumericDocValue(new Term("key", "none"), "n", 7);
      w.updateNumericDocValue(new Term("key", "k1"), "n", 9);
      releaseMerge.countDown();
      merger.join();
      releasePacket.countDown();
      updater.join();
      w.commit();

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        int doc =
            new IndexSearcher(r).search(new TermQuery(new Term("key", "k0")), 1).scoreDocs[0].doc;
        NumericDocValues n = MultiDocValues.getNumericValues(r, "n");
        assertTrue(n.advanceExact(doc));
        // expected: the last update of the k0 doc (n=2) survives the merge; actual: the older n=1
        assertEquals(2, n.longValue());
      }
    }
  }

  private static Document doc(String key) {
    Document doc = new Document();
    doc.add(new StringField("key", key, Field.Store.NO));
    doc.add(new NumericDocValuesField("n", 0));
    return doc;
  }

  private interface IOAction {
    void run() throws IOException;
  }

  private static void run(IOAction action) {
    try {
      action.run();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue("timed out", latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }
}
