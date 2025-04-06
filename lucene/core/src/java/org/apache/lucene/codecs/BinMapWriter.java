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

import java.io.IOException;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.codecs.CodecUtil;

/**
 * Writes a binmap file that maps docIDs to bin IDs.
 */
public final class BinMapWriter implements AutoCloseable {

  public static final String EXTENSION = "binmap";
  public static final int VERSION_START = 0;
  private static final String CODEC_NAME = "BinMap";

  private final Directory directory;
  private final SegmentWriteState state;
  private final int maxDoc;
  private final int binCount;
  private final int[] bins;

  public BinMapWriter(Directory directory, SegmentWriteState state, int[] docToBin, int binCount) {
    this.directory = directory;
    this.state = state;
    this.maxDoc = state.segmentInfo.maxDoc();
    this.binCount = binCount;
    this.bins = docToBin;
  }

  /**
   * Returns the expected binmap file name for this writer's segment.
   */
  public String getBinMapFileName() {
    return IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
  }

  @Override
  public void close() throws IOException {
    final String fileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, EXTENSION);
    IndexOutput out = null;
    boolean success = false;
    try {
      out = directory.createOutput(fileName, state.context);
      CodecUtil.writeIndexHeader(out, CODEC_NAME, VERSION_START,
          state.segmentInfo.getId(), state.segmentSuffix);
      out.writeInt(maxDoc);
      out.writeInt(binCount);
      for (int i = 0; i < maxDoc; i++) {
        out.writeInt(bins[i]);
      }
      CodecUtil.writeFooter(out);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(out);
      } else {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
}