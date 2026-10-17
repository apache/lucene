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

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexWriterRollbackReaderPool extends LuceneTestCase {

  public void testRollbackUncommittedMergedSegment() throws Exception {
    try (MockDirectoryWrapper dir =
        new MockDirectoryWrapper(random(), new ByteBuffersDirectory())) {
      dir.setAssertNoDeleteOpenFile(true);
      dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);

      LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
      mergePolicy.setMergeFactor(100);

      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(new Lucene104Codec())
              .setMergeScheduler(new SerialMergeScheduler())
              .setMergePolicy(mergePolicy)
              .setUseCompoundFile(true)
              .setMaxBufferedDocs(2)
              .setCommitOnClose(false);
      config.getCodec().compoundFormat().setCfsThresholdDocSize(Integer.MAX_VALUE);

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < 4; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.commit();

        // Commit two segments, then replace them with an uncommitted compound segment.
        try (DirectoryReader committed = DirectoryReader.open(dir)) {
          assertEquals(4, committed.numDocs());
          assertEquals(2, committed.leaves().size());
        }
        writer.forceMerge(1);

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(4, reader.numDocs());
          assertEquals(1, reader.leaves().size());
          SegmentReader segmentReader = (SegmentReader) reader.leaves().getFirst().reader();
          assertTrue(segmentReader.getSegmentInfo().info.getUseCompoundFile());
        }
      }

      try (DirectoryReader committed = DirectoryReader.open(dir)) {
        assertEquals(4, committed.numDocs());
        assertEquals(2, committed.leaves().size());
      }
    }
  }
}
