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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;

/** Tests indexwriter's infostream */
public class TestInfoStream extends LuceneTestCase {

  /** we shouldn't have test points unless we ask */
  public void testTestPointsOff() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setInfoStream(
        new InfoStream() {
          @Override
          public void close() throws IOException {}

          @Override
          public void message(String component, String message) {
            assertFalse("TP".equals(component));
          }

          @Override
          public boolean isEnabled(String component) {
            assertFalse("TP".equals(component));
            return true;
          }
        });
    IndexWriter iw = new IndexWriter(dir, iwc);
    iw.addDocument(new Document());
    iw.close();
    dir.close();
  }

  /** but they should work when we need */
  public void testTestPointsOn() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    AtomicBoolean seenTestPoint = new AtomicBoolean();
    iwc.setInfoStream(
        new InfoStream() {
          @Override
          public void close() throws IOException {}

          @Override
          public void message(String component, String message) {
            if ("TP".equals(component)) {
              seenTestPoint.set(true);
            }
          }

          @Override
          public boolean isEnabled(String component) {
            return true;
          }
        });
    IndexWriter iw =
        new IndexWriter(dir, iwc) {
          @Override
          protected boolean isEnableTestPoints() {
            return true;
          }
        };
    iw.addDocument(new Document());
    iw.close();
    dir.close();
    assertTrue(seenTestPoint.get());
  }

  public void testMergeSmallIndex() throws Exception {
    // examine info stream output from IW, MP, MS
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    Set<String> components = Set.of("IW", "MP", "MS", "BD", "BU");
    List<String> infoStream = new ArrayList<>();
    iwc.setInfoStream(
        new InfoStream() {
          @Override
          public void close() throws IOException {}

          @Override
          public void message(String component, String message) {
            if (components.contains(component)) {
              infoStream.add(String.format(Locale.ROOT, "[%s] %s\n", component, message));
            }
          }

          @Override
          public boolean isEnabled(String component) {
            return components.contains(component);
          }
        });
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      iw.addDocument(new Document());
      iw.commit();
      iw.addDocument(new Document());
      iw.addDocument(new Document());
      try (IndexReader reader = DirectoryReader.open(iw)) {
        assertTrue(iw.tryDeleteDocument(reader, 2) > 0);
      }
      iw.commit();
      iw.forceMerge(1);
    }
    for (String message : infoStream) {
      // we don't want to be printing full diagnostics every time a segment is mentioned in the log,
      // only when it is first flushed
      if (message.contains("diagnostics")) {
        assertTrue(message.startsWith("[IW] publishFlushedSegment"));
      }
      // System.out.print(message);
    }
  }
}
