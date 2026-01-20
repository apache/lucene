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

  /** Default document count threshold for using compound files with LogDocMergePolicy */
  static final int DEFAULT_CFS_THRESHOLD_DOC_SIZE = 65536; // docs

  /** Default byte size threshold for using compound files with other merge policies (64MB) */
  static final long DEFAULT_CFS_THRESHOLD_BYTE_SIZE = 64L * 1024 * 1024; // 64MB

  /** Default maximum segment size allowed for compound files (no limit) */
  static final long DEFAULT_MAX_CFS_SEGMENT_SIZE = Long.MAX_VALUE;

  /** Document count threshold for LogDocMergePolicy */
  private int cfsThresholdDocSize = DEFAULT_CFS_THRESHOLD_DOC_SIZE;

  /** Byte size threshold for other merge policies */
  private long cfsThresholdByteSize = DEFAULT_CFS_THRESHOLD_BYTE_SIZE;

  /** Whether compound files should be used at all */
  private boolean shouldUseCompoundFile = true;

  /** Maximum segment size that can be stored as compound file */
  private long maxCFSSegmentSize = DEFAULT_MAX_CFS_SEGMENT_SIZE;

  /**
   * Sets the document count threshold for using compound files with LogDocMergePolicy. Segments
   * with document count less than or equal to this threshold will use compound files.
   *
   * @param threshold the document count threshold
   */
  public void setCfsThresholdDocSize(int threshold) {
    this.cfsThresholdDocSize = threshold;
  }

  /**
   * Sets the byte size threshold for using compound files with merge policies other than
   * LogDocMergePolicy. Segments with size less than or equal to this threshold will use compound
   * files.
   *
   * @param thresholdBytes the byte size threshold in bytes
   */
  public void setCfsThresholdByteSize(long thresholdBytes) {
    this.cfsThresholdByteSize = thresholdBytes;
  }

  /**
   * Returns the current document count threshold for compound files.
   *
   * @return the document count threshold
   */
  public int getCfsThresholdDocSize() {
    return this.cfsThresholdDocSize;
  }

  /**
   * Returns the current byte size threshold for compound files.
   *
   * @return the byte size threshold in bytes
   */
  public long getCfsThresholdByteSize() {
    return this.cfsThresholdByteSize;
  }

  /**
   * Enables or disables the use of compound files entirely. When disabled, no segments will use
   * compound files regardless of other settings.
   *
   * @param useCompoundFile true to enable compound files, false to disable
   */
  public void setShouldUseCompoundFile(boolean useCompoundFile) {
    this.shouldUseCompoundFile = useCompoundFile;
  }

  /**
   * Returns whether compound files are enabled.
   *
   * @return true if compound files are enabled, false otherwise
   */
  public boolean getShouldUseCompoundFile() {
    return this.shouldUseCompoundFile;
  }

  /**
   * Returns the largest size allowed for a compound file segment in megabytes. Segments larger than
   * this size will not use compound files even if otherwise eligible.
   *
   * @return the maximum compound file segment size in MB
   */
  public double getMaxCFSSegmentSizeMB() {
    return maxCFSSegmentSize / 1024. / 1024.;
  }

  /**
   * Sets the maximum size limit for compound file segments in megabytes. If a merged segment will
   * be larger than this value, it will be left as a non-compound file even if compound files are
   * enabled. Set this to Double.POSITIVE_INFINITY (default) to always use CFS when other conditions
   * are met.
   *
   * @param v the maximum segment size in MB (must be >= 0)
   * @throws IllegalArgumentException if v is negative
   */
  public void setMaxCFSSegmentSizeMB(double v) {
    if (v < 0.0) {
      throw new IllegalArgumentException("maxCFSSegmentSizeMB must be >=0 (got " + v + ")");
    }
    v *= 1024 * 1024; // Convert MB to bytes
    this.maxCFSSegmentSize = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
  }

  /**
   * Determines whether a segment should use the compound file format based on its size and merge
   * policy.
   *
   * <p>The decision logic is as follows:
   *
   * <ol>
   *   <li>If compound files are disabled globally, return false
   *   <li>If segment size exceeds the maximum CFS segment size, return false
   *   <li>For DOCS-based policies: use CFS if document count ≤ document threshold
   *   <li>For BYTES-based policies: use CFS if byte size ≤ byte threshold
   * </ol>
   *
   * @param mergedInfoSize the size of the segment (document count for DOCS-based policies, bytes
   *     for BYTES-based policies)
   * @param mergePolicy the merge policy being used
   * @return true if the segment should use compound file format, false otherwise
   * @throws IOException if an I/O error occurs
   */
  public boolean useCompoundFile(long mergedInfoSize, MergePolicy mergePolicy) throws IOException {
    // Check if compound files are globally disabled
    if (this.shouldUseCompoundFile == false) {
      return false;
    }

    // Check if segment exceeds maximum allowed size for CFS
    if (mergedInfoSize > maxCFSSegmentSize) {
      return false;
    }

    // Apply appropriate threshold based on merge policy's size unit
    if (mergePolicy.getSizeUnit() == MergePolicy.SizeUnit.DOCS) {
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
