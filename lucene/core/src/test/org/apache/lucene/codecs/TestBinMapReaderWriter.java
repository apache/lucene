/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Unit tests for {@link BinMapWriter} and {@link BinMapReader} using {@link
 * DocBinningGraphBuilder}.
 */
public class TestBinMapReaderWriter extends LuceneTestCase {

  private Directory dir;
  private SegmentWriteState writeState;
  private SegmentReadState readState;

  @Before
  public void setUpTest() throws Exception {

    super.setUp();
    Path tempDir = createTempDir("binmap-test");
    dir = FSDirectory.open(tempDir);
    SegmentInfo info =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_1",
            16,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    readState = new SegmentReadState(dir, info, null, newIOContext(random()));
    writeState = new SegmentWriteState(null, dir, info, null, null, IOContext.DEFAULT);
  }

  @After
  public void tearDownTest() throws Exception {
    super.tearDown();
    dir.close();
  }

  @Test
  public void testBinMapWriteAndRead() throws IOException {
    final int maxDoc = writeState.segmentInfo.maxDoc();
    final int numBins = 4;

    InMemorySparseEdgeGraph graph = new InMemorySparseEdgeGraph();
    for (int i = 0; i < maxDoc; i++) {
      if (i < maxDoc - 1) {
        graph.addEdge(i, i + 1, 1.0f);
      }
      // ensure all nodes are initialized in the graph
      graph.ensureVertex(i);
    }

    int[] docToBin = DocBinningGraphBuilder.computeBins(graph, maxDoc, numBins);
    BinMapWriter writer = new BinMapWriter(writeState.directory, writeState, docToBin, numBins);

    writer.close();

    try (BinMapReader reader = new BinMapReader(dir, readState)) {
      assertEquals("bin count mismatch", numBins, reader.getBinCount());
      for (int i = 0; i < maxDoc; i++) {
        assertEquals("bin mismatch at docID " + i, docToBin[i], reader.getBin(i));
      }
    }
  }

  @Test
  public void testEmptySegment() throws IOException {
    Path emptyDir = createTempDir("binmap-empty");
    Directory directory = FSDirectory.open(emptyDir);

    SegmentInfo emptyInfo =
        new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_empty",
            0,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);

    SegmentWriteState emptyWriteState =
        new SegmentWriteState(null, directory, emptyInfo, null, null, IOContext.DEFAULT);
    SegmentReadState emptyReadState =
        new SegmentReadState(directory, emptyInfo, null, IOContext.DEFAULT);

    int[] emptyBinArray = {
      /*nothing in here = array with no slots*/
    };

    // Nothing to write since maxDoc is 0, but simulate writing header
    BinMapWriter writer =
        new BinMapWriter(emptyWriteState.directory, emptyWriteState, emptyBinArray, 0);
    writer.close();

    // Reader should read back bin count as 0
    try (BinMapReader reader = new BinMapReader(directory, emptyReadState)) {
      assertEquals(0, reader.getBinCount());
    }

    directory.close();
  }
}
