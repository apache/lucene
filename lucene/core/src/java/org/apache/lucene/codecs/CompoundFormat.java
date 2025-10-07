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
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Encodes/decodes compound files
 *
 * @lucene.experimental
 */
public abstract class CompoundFormat {
  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  // Explicitly declared so that we have non-empty javadoc
  protected CompoundFormat() {}

  // TODO: this is very minimal. If we need more methods,
  // we can add 'producer' classes.

  // Default thresholds
  static final int DEFAULT_CFS_THRESHOLD_DOC_SIZE = 65536; // docs
  static final long DEFAULT_CFS_THRESHOLD_BYTE_SIZE = 64L * 1024 * 1024; // 64MB

  /** Default max segment size allowed for CFS (bytes) */
  static final long DEFAULT_MAX_CFS_SEGMENT_SIZE = Long.MAX_VALUE;

  private int cfsThresholdDocSize = DEFAULT_CFS_THRESHOLD_DOC_SIZE;
  private long cfsThresholdByteSize = DEFAULT_CFS_THRESHOLD_BYTE_SIZE;
  private boolean shouldUseCompoundFile = true;
  private long maxCFSSegmentSize = DEFAULT_MAX_CFS_SEGMENT_SIZE;

  public void setCfsThresholdDocSize(int threshold) {
    this.cfsThresholdDocSize = threshold;
  }

  public void setCfsThresholdByteSize(long thresholdBytes) {
    this.cfsThresholdByteSize = thresholdBytes;
  }

  public int getCfsThresholdDocSize() {
    return this.cfsThresholdDocSize;
  }

  public long getCfsThresholdByteSize() {
    return this.cfsThresholdByteSize;
  }

  public void setShouldUseCompoundFile(boolean useCompoundFile) {
    this.shouldUseCompoundFile = useCompoundFile;
  }

  public boolean getShouldUseCompoundFile() {
    return this.shouldUseCompoundFile;
  }

  /** Returns the largest size allowed for a compound file segment (in MB) */
  public double getMaxCFSSegmentSizeMB() {
    return maxCFSSegmentSize / 1024. / 1024.;
  }

  /**
   * If a merged segment will be more than this value, leave the segment as non-compound file even
   * if compound file is enabled. Set this to Double.POSITIVE_INFINITY (default) to always use CFS.
   */
  public void setMaxCFSSegmentSizeMB(double v) {
    if (v < 0.0) {
      throw new IllegalArgumentException("maxCFSSegmentSizeMB must be >=0 (got " + v + ")");
    }
    v *= 1024 * 1024;
    this.maxCFSSegmentSize = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
  }

  /**
   * Returns true if a new segment (regardless of its origin) should use the compound file format.
   * The default implementation uses fixed thresholds:
   * - for LogDocMergePolicy, all segments with less than equals to {@link #getCfsThresholdDocSize()} docs;
   * - for byte-size-based merge policies, all segments with less than equals to {@link #getCfsThresholdByteSize()} bytes.
   * Additionally, segments larger than {@link #maxCFSSegmentSize} will not use CFS.
   */
  public boolean useCompoundFile(long mergedInfoSize, MergePolicy mergePolicy) throws IOException {
    if (this.shouldUseCompoundFile == false) {
      return false;
    }
    if (mergedInfoSize > maxCFSSegmentSize) {
      return false;
    }
    if (mergePolicy instanceof LogDocMergePolicy) {
      return mergedInfoSize <= this.cfsThresholdDocSize;
    } else {
      return mergedInfoSize <= this.cfsThresholdByteSize;
    }
  }

  /** Returns a Directory view (read-only) for the compound files in this segment */
  public abstract CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si)
      throws IOException;

  /**
   * Packs the provided segment's files into a compound format. All files referenced by the provided
   * {@link SegmentInfo} must have {@link CodecUtil#writeIndexHeader} and {@link
   * CodecUtil#writeFooter}.
   */
  public abstract void write(Directory dir, SegmentInfo si, IOContext context) throws IOException;
}
