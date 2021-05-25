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

package org.apache.lucene.backward_codecs.lucene86;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

/** Writable version of Lucene86SegmentInfoFormat for testing */
public class Lucene86RWSegmentInfoFormat extends Lucene86SegmentInfoFormat {

  /** Sole constructor. */
  public Lucene86RWSegmentInfoFormat() {}

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", SI_EXTENSION);

    try (IndexOutput output = EndiannessReverserUtil.createOutput(dir, fileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(fileName);
      CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, si.getId(), "");

      writeSegmentInfo(output, si);

      CodecUtil.writeFooter(output);
    }
  }

  private void writeSegmentInfo(DataOutput output, SegmentInfo si) throws IOException {
    Version version = si.getVersion();
    if (version.major < 7) {
      throw new IllegalArgumentException(
          "invalid major version: should be >= 7 but got: " + version.major + " segment=" + si);
    }
    // Write the Lucene version that created this segment, since 3.1
    output.writeInt(version.major);
    output.writeInt(version.minor);
    output.writeInt(version.bugfix);

    // Write the min Lucene version that contributed docs to the segment, since 7.0
    if (si.getMinVersion() != null) {
      output.writeByte((byte) 1);
      Version minVersion = si.getMinVersion();
      output.writeInt(minVersion.major);
      output.writeInt(minVersion.minor);
      output.writeInt(minVersion.bugfix);
    } else {
      output.writeByte((byte) 0);
    }

    assert version.prerelease == 0;
    output.writeInt(si.maxDoc());

    output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
    output.writeMapOfStrings(si.getDiagnostics());
    Set<String> files = si.files();
    for (String file : files) {
      if (!IndexFileNames.parseSegmentName(file).equals(si.name)) {
        throw new IllegalArgumentException(
            "invalid files: expected segment=" + si.name + ", got=" + files);
      }
    }
    output.writeSetOfStrings(files);
    output.writeMapOfStrings(si.getAttributes());

    Sort indexSort = si.getIndexSort();
    int numSortFields = indexSort == null ? 0 : indexSort.getSort().length;
    output.writeVInt(numSortFields);
    for (int i = 0; i < numSortFields; ++i) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalArgumentException("cannot serialize SortField " + sortField);
      }
      output.writeString(sorter.getProviderName());
      SortFieldProvider.write(sortField, output);
    }
  }
}
