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

import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestRollback extends LuceneTestCase {

  // LUCENE-2536
  public void testRollbackIntegrityWithBufferFlush() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter rw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(newStringField("pk", Integer.toString(i), Field.Store.YES));
      rw.addDocument(doc);
    }
    rw.close();

    // If buffer size is small enough to cause a flush, errors ensue...
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMaxBufferedDocs(2)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND));

    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      String value = Integer.toString(i);
      doc.add(newStringField("pk", value, Field.Store.YES));
      doc.add(newStringField("text", "foo", Field.Store.YES));
      w.updateDocument(new Term("pk", value), doc);
    }
    w.rollback();

    IndexReader r = DirectoryReader.open(dir);
    assertEquals("index should contain same number of docs post rollback", 5, r.numDocs());
    r.close();
    dir.close();
  }

  // A writer opened on an older commit rolls back to that commit's segments. The default deletion
  // policy deletes that commit when the writer is created. Once the writer merged the segments
  // away, only a rollback still needs their files, so they must still exist then.
  public void testRollbackAfterMergingAwayDeletedStartingCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE)
                .setMergePolicy(new TieredMergePolicy()));
    for (int i = 0; i < 2; i++) {
      w.addDocument(new Document());
      w.commit();
    }
    IndexCommit olderCommit = DirectoryReader.listCommits(dir).get(1);
    w.forceMerge(1);
    w.close(); // the latest commit has the merged segment only

    w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                .setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy())
                .setMergePolicy(new TieredMergePolicy())
                .setIndexCommit(olderCommit));
    assertFalse(slowFileExists(dir, olderCommit.getSegmentsFileName()));
    w.addDocument(new Document());
    w.forceMerge(1);
    w.rollback();

    try (DirectoryReader r = DirectoryReader.open(dir)) {
      assertEquals(2, r.numDocs());
    }
    dir.close();
  }

  /**
   * An NRT reader kept after its writer was rolled back must not be current once a later writer
   * commits, even if that commit has the same version as the reader: the later writer counts
   * versions again from the last commit, so it can reach a version that the rolled back writer gave
   * to this reader, with different documents.
   */
  public void testNRTReaderOfRolledBackWriterAfterNextWriterCommits() throws Exception {
    for (int refreshes = 0; refreshes <= 8; refreshes++) {
      for (int openMode = 0; openMode < 3; openMode++) {
        boolean viaClose = random().nextBoolean();
        Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir,
                newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setCommitOnClose(viaClose == false));
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        for (int i = 0; i < refreshes; i++) {
          writer.addDocument(new Document());
          DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer);
          reader.close();
          reader = newReader;
        }
        if (viaClose) {
          writer.close();
        } else {
          writer.rollback();
        }
        assertEquals(refreshes == 0, reader.isCurrent());

        IndexWriterConfig config =
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
        switch (openMode) {
          case 0 -> config.setOpenMode(OpenMode.APPEND);
          case 1 -> config.setOpenMode(OpenMode.CREATE);
          default -> {
            List<IndexCommit> commits = DirectoryReader.listCommits(dir);
            config.setIndexCommit(commits.get(commits.size() - 1));
          }
        }
        try (IndexWriter next = new IndexWriter(dir, config)) {
          for (int i = 0; i < 8; i++) {
            next.addDocument(new Document());
            next.commit();
            assertFalse(
                "refreshes="
                    + refreshes
                    + " commit="
                    + i
                    + " reader version="
                    + reader.getVersion()
                    + " latest commit version="
                    + SegmentInfos.readLatestCommit(dir).getVersion(),
                reader.isCurrent());
          }
        }
        reader.close();
        dir.close();
      }
    }
  }
}
