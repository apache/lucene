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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.index.BaseCompoundFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene90CompoundFormat extends BaseCompoundFormatTestCase {
  private final Codec codec = TestUtil.getDefaultCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testFileLengthOrdering() throws IOException {
    Directory dir = newDirectory();
    // Setup the test segment
    String segment = "_123";
    int chunk = 1024; // internal buffer size used by the stream
    SegmentInfo si = newSegmentInfo(dir, segment);
    byte[] segId = si.getId();
    List<String> orderedFiles = new ArrayList<>();
    int randomFileSize = random().nextInt(chunk);
    for (int i = 0; i < 10; i++) {
      String filename = segment + "." + i;
      createRandomFile(dir, filename, randomFileSize, segId);
      // increase the next files size by a random amount
      randomFileSize += random().nextInt(100) + 1;
      orderedFiles.add(filename);
    }
    List<String> shuffledFiles = new ArrayList<>(orderedFiles);
    Collections.shuffle(shuffledFiles, random());
    si.setFiles(shuffledFiles);
    si.getCodec().compoundFormat().write(dir, si, IOContext.DEFAULT);

    // entries file should contain files ordered by their size
    String entriesFileName =
        IndexFileNames.segmentFileName(si.name, "", Lucene90CompoundFormat.ENTRIES_EXTENSION);
    try (ChecksumIndexInput entriesStream =
        dir.openChecksumInput(entriesFileName, IOContext.READ)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            entriesStream,
            Lucene90CompoundFormat.ENTRY_CODEC,
            Lucene90CompoundFormat.VERSION_START,
            Lucene90CompoundFormat.VERSION_CURRENT,
            si.getId(),
            "");
        final int numEntries = entriesStream.readVInt();
        long lastOffset = 0;
        long lastLength = 0;
        for (int i = 0; i < numEntries; i++) {
          final String id = entriesStream.readString();
          assertEquals(orderedFiles.get(i), segment + id);
          long offset = entriesStream.readLong();
          assertTrue(offset > lastOffset);
          lastOffset = offset;
          long length = entriesStream.readLong();
          assertTrue(length >= lastLength);
          lastLength = length;
        }
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(entriesStream, priorE);
      }
    }
    dir.close();
  }
}
