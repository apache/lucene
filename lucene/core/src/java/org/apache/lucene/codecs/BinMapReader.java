package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;

/** Reads bin ID assignments per doc from binmap file. */
public final class BinMapReader implements Closeable {

  private static final String EXTENSION = "binmap";
  private static final int VERSION_START = 0;
  private static final String CODEC_NAME = "BinMap";

  private final int[] bins;
  private final int binCount;

  public BinMapReader(Directory dir, SegmentReadState state) throws IOException {
    final String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);

    ChecksumIndexInput in = dir.openChecksumInput(fileName);
    boolean success = false;
    try {
      CodecUtil.checkIndexHeader(
          in, CODEC_NAME, VERSION_START, VERSION_START,
          state.segmentInfo.getId(), state.segmentSuffix);

      int maxDoc = in.readInt();
      this.binCount = in.readInt();

      if (maxDoc != state.segmentInfo.maxDoc()) {
        throw new IOException("maxDoc mismatch: expected=" + state.segmentInfo.maxDoc() + " actual=" + maxDoc);
      }

      this.bins = new int[maxDoc];
      for (int i = 0; i < maxDoc; i++) {
        bins[i] = in.readInt();
      }

      CodecUtil.checkFooter(in);
      success = true;
    } finally {
      if (success == false) {
        try {
          in.close();
        } catch (Throwable t) {
          // suppress
        }
      } else {
        in.close();
      }
    }
  }

  public int getBin(int docID) {
    return bins[docID];
  }

  public int getBinCount() {
    return binCount;
  }

  public int[] getBinArrayCopy() {
    return Arrays.copyOf(bins, bins.length);
  }

  @Override
  public void close() {}
}