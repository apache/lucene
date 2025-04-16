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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Writes a binmap file for a segment.
 *
 * <p>The binmap assigns each document in the segment to a bin ID, typically used for scoring or
 * early termination. The output is a binary file storing one integer bin ID per document.
 */
public final class BinMapWriter implements AutoCloseable {

  /** File extension for binmap files. */
  public static final String EXTENSION = "binmap";

  /** Initial file format version. */
  public static final int VERSION_START = 0;

  private static final String CODEC_NAME = "BinMap";

  private final Directory directory;
  private final SegmentWriteState state;
  private final int maxDoc;
  private final int binCount;
  private final int[] bins;

  /**
   * Creates a new {@link BinMapWriter} instance.
   *
   * @param directory output directory
   * @param state current segment write state
   * @param docToBin mapping from docID to bin ID
   * @param binCount total number of bins
   */
  public BinMapWriter(Directory directory, SegmentWriteState state, int[] docToBin, int binCount) {
    this.directory = directory;
    this.state = state;
    this.maxDoc = state.segmentInfo.maxDoc();
    this.binCount = binCount;
    this.bins = docToBin;
  }

  /**
   * Writes the binmap file to the index output directory.
   *
   * <p>The format is:
   *
   * <ul>
   *   <li>Header (versioned with {@link org.apache.lucene.codecs.CodecUtil})
   *   <li>maxDoc (int)
   *   <li>binCount (int)
   *   <li>{@code maxDoc} bin IDs, one int per document
   *   <li>Footer
   * </ul>
   */
  @Override
  public void close() throws IOException {
    final String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
    IndexOutput out = null;
    boolean success = false;
    try {
      out = directory.createOutput(fileName, state.context);
      CodecUtil.writeIndexHeader(
          out, CODEC_NAME, VERSION_START, state.segmentInfo.getId(), state.segmentSuffix);
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
