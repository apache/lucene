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
package org.apache.lucene.codecs;

import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;

import java.io.IOException;

public class TestCompoundFormat extends LuceneTestCase {

  private CompoundFormat format;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    format = new CompoundFormat() {
      @Override
      public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo segmentInfo) {
        return null;
      }
      @Override
      public void write(Directory dir, SegmentInfo segmentInfo, IOContext context) {
        // No-op
      }
    };
  }

  public void testDefaultThresholds() throws IOException {
    format.setShouldUseCompoundFile(true);
    format.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY); // Always allow

    MergePolicy docPolicy = new LogDocMergePolicy();
    MergePolicy bytePolicy = new TieredMergePolicy();

    // Should use compound file for doc policy under threshold
    assertTrue(format.useCompoundFile(65536, docPolicy));
    assertFalse(format.useCompoundFile(65537, docPolicy));

    // Should use compound file for byte policy under threshold (64MB)
    assertTrue(format.useCompoundFile(64L * 1024 * 1024, bytePolicy));
    assertFalse(format.useCompoundFile((64L * 1024 * 1024) + 1, bytePolicy));
  }

  public void testDisabledCompoundFile() throws IOException {
    format.setShouldUseCompoundFile(false);
    MergePolicy docPolicy = new LogDocMergePolicy();

    // Should never use compound file if disabled
    assertFalse(format.useCompoundFile(1, docPolicy));
    assertFalse(format.useCompoundFile(65536, docPolicy));
  }

  public void testMaxCFSSegmentSize() throws IOException {
    format.setShouldUseCompoundFile(true);
    format.setMaxCFSSegmentSizeMB(10); // 10MB
    MergePolicy bytePolicy = new TieredMergePolicy();

    // Should skip CFS if over maxCFSSegmentSize
    assertTrue(format.useCompoundFile(9L * 1024 * 1024, bytePolicy));
    assertFalse(format.useCompoundFile(11L * 1024 * 1024, bytePolicy));
  }

  public void testCustomThresholds() throws IOException {
    format.setCfsThresholdDocSize(1000);
    format.setCfsThresholdByteSize(10 * 1024 * 1024); // 10MB

    MergePolicy docPolicy = new LogDocMergePolicy();
    MergePolicy bytePolicy = new TieredMergePolicy();

    assertTrue(format.useCompoundFile(1000, docPolicy));
    assertFalse(format.useCompoundFile(1001, docPolicy));

    assertTrue(format.useCompoundFile(10 * 1024 * 1024, bytePolicy));
    assertFalse(format.useCompoundFile((10 * 1024 * 1024) + 1, bytePolicy));
  }
}
