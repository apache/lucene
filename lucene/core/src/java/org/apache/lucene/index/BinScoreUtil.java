package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Random;

import org.apache.lucene.codecs.BinMapReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

/** Utilities for wrapping readers with binning support. */
public final class BinScoreUtil {

  private static final Map<IndexReader, List<Closeable>> compoundReaders = new WeakHashMap<>();
  private static final long SHUFFLE_SEED = 42L;

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

  /** Shuffle leaves before wrapping to reduce early-termination bias. */
  private static CompositeReader wrapComposite(CompositeReader reader) throws IOException {
    List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());
    Collections.shuffle(leaves, new Random(SHUFFLE_SEED));
    List<LeafReader> wrapped = new ArrayList<>();
    for (LeafReaderContext ctx : leaves) {
      wrapped.add(wrap(ctx.reader()));
    }
    return new MultiReader(wrapped.toArray(new LeafReader[0]), true);
  }

  /** Wrap a LeafReader with bin score logic if binmap file is found. */
  public static LeafReader wrap(LeafReader reader) throws IOException {
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

    String suffix = binmapFile.substring(name.length() + 1, binmapFile.length() - ".binmap".length());
    SegmentReadState state = new SegmentReadState(dir, info, sr.getFieldInfos(), IOContext.READONCE, suffix);

    BinMapReader binMap = null;
    try {
      binMap = new BinMapReader(dir, state);
      BinScoreReader binScore = new BinScoreReader(binMap);
      final Closeable closeOnClose = compoundReader;

      return new BinScoreLeafReader(reader, binScore, binMap) {
        @Override
        protected void doClose() throws IOException {
          IOUtils.close(closeOnClose);
          super.doClose();
        }
      };
    } catch (Throwable t) {
      IOUtils.closeWhileHandlingException(binMap, compoundReader);
      throw t;
    }
  }

  private static SegmentReader getSegmentReader(LeafReader reader) {
    if (reader instanceof SegmentReader) return (SegmentReader) reader;
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