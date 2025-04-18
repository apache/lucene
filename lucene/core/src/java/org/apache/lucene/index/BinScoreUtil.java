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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.WeakHashMap;
import org.apache.lucene.codecs.BinMapReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

/**
 * Utility methods for wrapping readers with bin-aware scoring support. Enables loading bin map
 * metadata and injecting scoring logic at search time.
 */
public final class BinScoreUtil {

  /** Tracks compound file readers for cleanup after wrapping. */
  private static final Map<IndexReader, List<Closeable>> compoundReaders = new WeakHashMap<>();

  /** Shuffle seed to reduce leaf bias in multi-segment readers. */
  private static final long SHUFFLE_SEED = 42L;

  private BinScoreUtil() {}

  /**
   * Wraps an IndexReader with bin-aware scoring if binmap metadata is present. Supports both
   * LeafReader and CompositeReader types.
   *
   * @param reader the input IndexReader
   * @return wrapped reader if binmap is found; otherwise original reader
   * @throws IOException on I/O error
   */
  public static IndexReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof LeafReader) {
      return wrap((LeafReader) reader);
    }
    if (reader instanceof CompositeReader) {
      return wrapComposite((CompositeReader) reader);
    }
    return reader;
  }

  /**
   * Wraps a CompositeReader (e.g., DirectoryReader) with bin-aware logic. Leaf readers are shuffled
   * to mitigate early termination bias, and each is wrapped individually. All associated compound
   * readers are tracked and must be closed via {@link #closeResources}.
   *
   * @param reader composite reader to wrap
   * @return MultiReader with bin-aware leaves
   * @throws IOException on error
   */
  private static CompositeReader wrapComposite(CompositeReader reader) throws IOException {
    List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());
    Collections.shuffle(leaves, new Random(SHUFFLE_SEED));
    List<LeafReader> wrapped = new ArrayList<>();
    List<Closeable> toClose = new ArrayList<>();

    for (LeafReaderContext ctx : leaves) {
      wrapped.add(wrap(ctx.reader(), toClose));
    }

    MultiReader multiReader = new MultiReader(wrapped.toArray(new LeafReader[0]), true);

    if (!toClose.isEmpty()) {
      synchronized (compoundReaders) {
        compoundReaders.put(multiReader, toClose);
      }
    }

    return multiReader;
  }

  /**
   * Wraps a single LeafReader with bin-aware scoring logic, collecting any compound readers created
   * for later cleanup.
   *
   * @param reader the input LeafReader
   * @param closables list to track compound readers for external cleanup
   * @return wrapped reader if binmap is found; otherwise original reader
   * @throws IOException on error
   */
  private static LeafReader wrap(LeafReader reader, List<Closeable> closables) throws IOException {
    SegmentReader sr = getSegmentReader(reader);
    if (sr == null) return reader;

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

      if (compoundReader != null) {
        closables.add(compoundReader);
      }

      return new BinScoreLeafReader(reader, binScore, binMap, compoundReader);
    } catch (Throwable t) {
      IOUtils.closeWhileHandlingException(binMap, compoundReader);
      throw t;
    }
  }

  /**
   * Wraps a LeafReader and tracks compound files internally. This method is intended for direct
   * wrapping of single-segment readers.
   *
   * @param reader input reader
   * @return bin-aware reader or original
   * @throws IOException on error
   */
  public static LeafReader wrap(LeafReader reader) throws IOException {
    return wrap(reader, new ArrayList<>());
  }

  /** Extracts the SegmentReader from the given reader if available. */
  private static SegmentReader getSegmentReader(LeafReader reader) {
    if (reader instanceof SegmentReader) return (SegmentReader) reader;
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    return (unwrapped instanceof SegmentReader) ? (SegmentReader) unwrapped : null;
  }

  /** Finds a binmap file in the given directory for the specified segment. */
  private static String findBinmapFile(Directory dir, String segmentName) throws IOException {
    for (String file : dir.listAll()) {
      if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
        return file;
      }
    }
    return null;
  }

  /**
   * Returns the BinScoreReader associated with a bin-aware wrapped reader.
   *
   * @param reader the LeafReader
   * @return BinScoreReader or null
   */
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

  /**
   * Closes all resources (e.g., compound readers) associated with the wrapped IndexReader. This
   * method must be invoked before or during IndexReader#close to ensure full cleanup.
   *
   * @param reader the top-level MultiReader returned by {@link #wrap(IndexReader)}
   * @throws IOException on error
   */
  public static void closeResources(IndexReader reader) throws IOException {
    synchronized (compoundReaders) {
      List<Closeable> list = compoundReaders.remove(reader);
      if (list != null) {
        IOUtils.close(list);
      }
    }
  }
}
