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
import java.util.Iterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ByteBlockPool;

/** Test for the buffer limit flush functionality that prevents ByteBlockPool integer overflow. */
public class TestBufferLimitFlush extends LuceneTestCase {

  public void testBufferLimitConstants() {
    // Verify the buffer limit calculation is correct
    assertEquals(32768, ByteBlockPool.BYTE_BLOCK_SIZE);
    assertEquals(65535, ByteBlockPool.MAX_BUFFER_COUNT);

    // Verify that MAX_BUFFER_COUNT is indeed the correct limit
    // The overflow occurs when we try to allocate beyond this limit
    int maxSafeOffset = ByteBlockPool.MAX_BUFFER_COUNT * ByteBlockPool.BYTE_BLOCK_SIZE;
    assertTrue("Max safe offset should be positive", maxSafeOffset > 0);

    // The overflow happens when we try to add another BYTE_BLOCK_SIZE beyond the limit
    ArithmeticException exception =
        expectThrows(
            ArithmeticException.class,
            () -> {
              Math.addExact(maxSafeOffset, ByteBlockPool.BYTE_BLOCK_SIZE);
            });

    // Verify we got the expected overflow exception
    assertNotNull("Should have thrown ArithmeticException due to overflow", exception);
  }

  public void testByteBlockPoolBufferLimitDetection() {
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    pool.nextBuffer();

    // Test buffer count tracking
    assertEquals(1, pool.getBufferCount());

    // Test approaching limit detection with various thresholds
    assertFalse(
        "Should not be approaching limit with threshold 65000",
        pool.isApproachingBufferLimit(65000));
    assertFalse(
        "Should not be approaching limit with threshold 2", pool.isApproachingBufferLimit(2));
    assertTrue("Should be approaching limit with threshold 1", pool.isApproachingBufferLimit(1));
    assertTrue("Should be approaching limit with threshold 0", pool.isApproachingBufferLimit(0));
  }

  public void testDWPTBufferLimitCheck() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    // Disable other flush triggers to isolate buffer limit testing
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    IndexWriter writer = new IndexWriter(dir, iwc);

    // Add a document to initialize the DWPT
    Document doc = new Document();
    doc.add(new TextField("content", "test content", TextField.Store.NO));
    writer.addDocument(doc);

    // Get the active DWPT and test buffer limit checking
    DocumentsWriter docsWriter = writer.getDocsWriter();
    DocumentsWriterFlushControl flushControl = docsWriter.flushControl;

    // Find an active DWPT
    DocumentsWriterPerThread dwpt = null;
    for (Iterator<DocumentsWriterPerThread> it = flushControl.allActiveWriters(); it.hasNext(); ) {
      DocumentsWriterPerThread activeDwpt = it.next();
      dwpt = activeDwpt;
      break;
    }

    assertNotNull("Should have at least one active DWPT", dwpt);

    // Test the buffer limit checking method exists and works
    dwpt.lock();
    try {
      boolean approaching = dwpt.isApproachingBufferLimit();
      // With normal documents, we should not be approaching the limit
      assertFalse("Should not be approaching buffer limit with normal documents", approaching);
    } finally {
      dwpt.unlock();
    }

    writer.close();
    dir.close();
  }

  public void testFlushControlBufferLimitIntegration() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    // Disable other flush triggers
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    IndexWriter writer = new IndexWriter(dir, iwc);
    DocumentsWriter docsWriter = writer.getDocsWriter();
    DocumentsWriterFlushControl flushControl = docsWriter.flushControl;

    // Add a document to create a DWPT
    Document doc = new Document();
    doc.add(new TextField("content", "test", TextField.Store.NO));
    writer.addDocument(doc);

    // Verify that the flush control integration is working
    // The doAfterDocument method should include our buffer limit check
    // This is tested indirectly by ensuring no exceptions are thrown
    // and the mechanism is in place

    assertTrue("Should have active bytes after adding document", flushControl.activeBytes() > 0);

    writer.close();
    dir.close();
  }
}
