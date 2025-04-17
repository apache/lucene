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
package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.BinMapReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

/** Utilities for wrapping readers with binning support. */
public final class BinScoreUtil {

  private BinScoreUtil() {}

  /** Wrap an IndexReader with bin support. */
  public static IndexReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof LeafReader) {
      return wrap((LeafReader) reader);
    }
    if (reader instanceof CompositeReader) {
      return wrapComposite((CompositeReader) reader);
    }
    return reader;
  }

  private static CompositeReader wrapComposite(CompositeReader reader) throws IOException {
    List<LeafReader> wrapped = new ArrayList<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      wrapped.add(wrap(ctx.reader()));
    }
    // This MultiReader just wraps the readers — responsibility for cleanup is delegated to
    // individual leaves.
    return new MultiReader(wrapped.toArray(new LeafReader[0]), true);
  }

  /** Wrap a LeafReader with bin score logic if binmap file is found. */
  public static LeafReader wrap(LeafReader reader) throws IOException {
    SegmentReader sr = getSegmentReader(reader);
    if (sr == null) {
      return reader;
    }

    SegmentCommitInfo commitInfo = sr.getSegmentInfo();
    SegmentInfo info = commitInfo.info;
    Directory dir = sr.directory();
    Closeable compoundReader = null;

    if (info.getUseCompoundFile()) {
      var format = info.getCodec().compoundFormat();
      if (format != null) {
        compoundReader = format.getCompoundReader(dir, info);
        dir = (Directory) compoundReader;
      }
    }

    String name = info.name;
    String binmapFile = findBinmapFile(dir, name);
    if (binmapFile == null) {
      IOUtils.closeWhileHandlingException(compoundReader);
      return reader;
    }

    String suffix =
        binmapFile.substring(name.length() + 1, binmapFile.length() - ".binmap".length());
    SegmentReadState state =
        new SegmentReadState(dir, info, sr.getFieldInfos(), IOContext.READONCE, suffix);

    BinMapReader binMap = null;
    try {
      binMap = new BinMapReader(dir, state);
      BinScoreReader binScore = new BinScoreReader(binMap);

      final Closeable closeOnClose = compoundReader;
      return new BinScoreLeafReader(reader, binScore, binMap) {
        @Override
        protected void doClose() throws IOException {
          IOUtils.close(closeOnClose); // closes compoundReader if present
          super.doClose(); // closes binMap, etc.
        }
      };
    } catch (Throwable t) {
      IOUtils.closeWhileHandlingException(binMap, compoundReader);
      throw t;
    }
  }

  private static SegmentReader getSegmentReader(LeafReader reader) {
    if (reader instanceof SegmentReader) {
      return (SegmentReader) reader;
    }
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    return (unwrapped instanceof SegmentReader) ? (SegmentReader) unwrapped : null;
  }

  private static String findBinmapFile(Directory dir, String segmentName) throws IOException {
    for (String file : dir.listAll()) {
      if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
        return file;
      }
    }
    return null;
  }

  /** Return bin score reader if available. */
  public static BinScoreReader getBinScoreReader(LeafReader reader) {
    if (reader instanceof BinScoreLeafReader) {
      return ((BinScoreLeafReader) reader).getBinScoreReader();
    }
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    if (unwrapped instanceof BinScoreLeafReader) {
      return ((BinScoreLeafReader) unwrapped).getBinScoreReader();
    }
    return null;
  }
}
