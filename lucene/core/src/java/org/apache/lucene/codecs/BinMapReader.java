/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.codecs;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Reads the binmap file for a segment and exposes per-document bin assignments.
 *
 * <p>Each document is assigned to a bin during indexing. The binmap file encodes a mapping from
 * docID to binID and is used at search time to access bin-specific metadata such as scoring boosts.
 *
 * <p>This class validates file integrity using Lucene’s codec header/footer checks.
 */
public final class BinMapReader implements Closeable {

  private static final String EXTENSION = "binmap";
  private static final int VERSION_START = 0;
  private static final String CODEC_NAME = "BinMap";

  private final int[] bins;
  private final int binCount;

  /**
   * Loads the binmap file for the given segment.
   *
   * @param dir directory containing the segment files
   * @param state read state for the segment
   * @throws IOException if the file cannot be read or fails validation
   */
  public BinMapReader(Directory dir, SegmentReadState state) throws IOException {
    final String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);

    ChecksumIndexInput in = null;
    boolean success = false;
    try {
      in = dir.openChecksumInput(fileName);
      CodecUtil.checkIndexHeader(
          in,
          CODEC_NAME,
          VERSION_START,
          VERSION_START,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      int maxDoc = in.readInt();
      this.binCount = in.readInt();

      if (maxDoc != state.segmentInfo.maxDoc()) {
        throw new IOException(
            "maxDoc mismatch: expected=" + state.segmentInfo.maxDoc() + " actual=" + maxDoc);
      }

      this.bins = new int[maxDoc];
      for (int i = 0; i < maxDoc; i++) {
        bins[i] = in.readInt();
      }

      CodecUtil.checkFooter(in);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  /**
   * Returns the bin ID assigned to the given document.
   *
   * @param docID document ID
   * @return the bin ID
   */
  public int getBin(int docID) {
    return bins[docID];
  }

  /** Returns the total number of bins used in this segment. */
  public int getBinCount() {
    return binCount;
  }

  /** Returns a copy of the full bin assignment array. */
  public int[] getBinArrayCopy() {
    int[] copy = new int[bins.length];
    System.arraycopy(bins, 0, copy, 0, bins.length);
    return copy;
  }

  @Override
  public void close() {}
}
