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
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestSortingStoredFieldsConsumer extends LuceneTestCase {

  /**
   * Verifies that when no stored fields are written, flushing the {@link
   * SortingStoredFieldsConsumer} skips per-document seeks and reads on the stored fields file. Only
   * the initial seek for checksum retrieval should occur.
   */
  public void testFlushWithNoStoredFieldsSkipsPerDocumentSeeks() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      Codec codec = iwc.getCodec();
      SegmentInfo si =
          new SegmentInfo(
              dir,
              Version.LATEST,
              null,
              "_0",
              -1,
              false,
              false,
              codec,
              Collections.emptyMap(),
              StringHelper.randomId(),
              new HashMap<>(),
              null);

      AtomicInteger numSeeks = new AtomicInteger();
      Directory trackingDir =
          new FilterDirectory(dir) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
              if (name.contains(Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION)) {
                return new FilterIndexInput(name, super.openInput(name, context)) {
                  @Override
                  public void seek(long pos) throws IOException {
                    numSeeks.incrementAndGet();
                    super.seek(pos);
                  }
                };
              }
              return super.openInput(name, context);
            }
          };

      SortingStoredFieldsConsumer consumer =
          new SortingStoredFieldsConsumer(codec, trackingDir, si);

      int numDocs = TestUtil.nextInt(random(), 1, 100);
      for (int i = 0; i < numDocs; i++) {
        consumer.startDocument(i);
        consumer.finishDocument();
      }
      consumer.finish(numDocs);

      si.setMaxDoc(numDocs);
      SegmentWriteState state =
          new SegmentWriteState(
              null,
              dir,
              si,
              new FieldInfos(new FieldInfo[0]),
              null,
              IOContext.flush(new FlushInfo(numDocs, 10)));

      // Identity mapping: doc order is unchanged
      Sorter.DocMap identityMap =
          new Sorter.DocMap() {
            @Override
            public int oldToNew(int docID) {
              return docID;
            }

            @Override
            public int newToOld(int docID) {
              return docID;
            }

            @Override
            public int size() {
              return numDocs;
            }
          };
      consumer.flush(state, identityMap);

      try (StoredFieldsReader reader =
          codec.storedFieldsFormat().fieldsReader(dir, si, state.fieldInfos, IOContext.READONCE)) {
        for (int i = 0; i < numDocs; i++) {
          Document document = reader.document(i);
          assertNotNull(document);
          assertTrue(document.getFields().isEmpty());
        }
      }

      assertEquals(
          "Expected only 1 seek (for checksum retrieval), not one per document", 1, numSeeks.get());
    }
  }
}
