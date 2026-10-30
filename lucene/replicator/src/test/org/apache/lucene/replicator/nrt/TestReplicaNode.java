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

package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestReplicaNode extends LuceneTestCase {

  /**
   * Implements just enough of {@link ReplicaNode} to exercise {@link ReplicaNode#getFilesToCopy}.
   */
  private static class MinimalReplicaNode extends ReplicaNode {
    MinimalReplicaNode(Directory dir) throws IOException {
      super(1, dir, new SearcherFactory(), null);
    }

    @Override
    protected CopyJob newCopyJob(
        String reason,
        Map<String, FileMetaData> files,
        Map<String, FileMetaData> prevFiles,
        boolean highPriority,
        CopyJob.OnceDone onceDone) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void launch(CopyJob job) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void sendNewReplica() {
      throw new UnsupportedOperationException();
    }
  }

  public void testRejectsFileNamesEscapingIndexDirectory() throws Exception {
    FileMetaData metaData = new FileMetaData(new byte[0], new byte[0], 0, 0);
    try (Directory dir = new ByteBuffersDirectory()) {
      ReplicaNode node = new MinimalReplicaNode(dir);
      for (String fileName :
          new String[] {
            "../segments_1", "..", ".", "", "sub/_0.cfs", "sub\\_0.cfs", "/tmp/_0.cfs", "c:_0.cfs"
          }) {
        IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class,
                () -> node.getFilesToCopy(Map.of(fileName, metaData)));
        assertTrue(e.getMessage().contains("invalid file name"));
      }
    }
  }

  public void testAcceptsPlainIndexFileNames() throws Exception {
    FileMetaData metaData = new FileMetaData(new byte[0], new byte[0], 0, 0);
    try (Directory dir = new ByteBuffersDirectory()) {
      ReplicaNode node = new MinimalReplicaNode(dir);
      List<Map.Entry<String, FileMetaData>> toCopy =
          node.getFilesToCopy(Map.of("_0.cfs", metaData, "segments_2", metaData));
      assertEquals(2, toCopy.size());
    }
  }
}
