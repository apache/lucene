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
package org.apache.lucene.codecs.simpletext;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.index.BaseSegmentInfoFormatTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/** Tests SimpleTextSegmentInfoFormat */
public class TestSimpleTextSegmentInfoFormat extends BaseSegmentInfoFormatTestCase {
  private final Codec codec = new SimpleTextCodec();

  @Override
  protected Version[] getVersions() {
    return new Version[] {Version.LATEST};
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testFileIsUTF8() throws IOException {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    byte[] id = StringHelper.randomId();
    SegmentInfo info =
        new SegmentInfo(
            dir,
            getVersions()[0],
            getVersions()[0],
            "_123",
            1,
            false,
            false,
            codec,
            Collections.<String, String>emptyMap(),
            id,
            Collections.emptyMap(),
            null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    String segFileName =
        IndexFileNames.segmentFileName("_123", "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(segFileName, IOContext.READONCE)) {
      long length = input.length();
      if (length > 5_000) {
        // Avoid allocating a huge array if the length is wrong
        fail("SegmentInfos should not be this large");
      }
      byte[] bytes = new byte[(int) length];
      BytesRef bytesRef = new BytesRef(bytes);
      // If the following are equal, it means the bytes were not well-formed UTF8.
      assertNotEquals(bytesRef.toString(), Term.toString(bytesRef));
    }
    dir.close();
  }
}
