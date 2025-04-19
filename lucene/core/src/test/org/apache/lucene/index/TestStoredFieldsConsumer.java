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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestStoredFieldsConsumer extends LuceneTestCase {

  public void testFinish() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            null,
            "_0",
            -1,
            false,
            false,
            iwc.getCodec(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);

    AtomicInteger startDocCounter = new AtomicInteger(), finishDocCounter = new AtomicInteger();
    StoredFieldsConsumer consumer =
        new StoredFieldsConsumer(iwc.getCodec(), dir, si) {
          @Override
          void startDocument(int docID) throws IOException {
            super.startDocument(docID);
            startDocCounter.incrementAndGet();
          }

          @Override
          void finishDocument() throws IOException {
            super.finishDocument();
            finishDocCounter.incrementAndGet();
          }
        };

    int numDocs = 3;
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
    consumer.flush(state, null);
    dir.close();

    assertEquals(numDocs, startDocCounter.get());
    assertEquals(numDocs, finishDocCounter.get());
  }
}
