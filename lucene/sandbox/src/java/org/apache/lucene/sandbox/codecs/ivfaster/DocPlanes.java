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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.IOException;

/**
 * Every document's coarse bit planes, packed once and reused by every consumer.
 *
 * <p>DOCUMENTS DO NOT MOVE BETWEEN LLOYD ITERATIONS, only centroids do, so a document's coarse code
 * is a fixed function of its rotated vector. Packing all of them once keeps routing, reaping and
 * spill selection on bit-work.
 *
 * <p>The buffer this class holds is also what the writer emits for the on-disk coarse section, so
 * every code is derived exactly once per build and the routing scan and the index agree by
 * construction.
 *
 * <h2>Layout</h2>
 *
 * <p>One contiguous buffer, {@code count} records of {@link #stride()} bytes. A record holds the
 * {@link Nitrox2#PLANES} bit planes concatenated, plane {@code t} at {@code t * planeBytes}, then
 * padding out to the next 64-byte boundary:
 *
 * <pre>
 *   record i:  [ plane 0: planeBytes ] ... [ plane PLANES-1: planeBytes ][ pad to 64 B ]
 * </pre>
 *
 * <p>THE PLANE COUNT IS CONFIGURED. {@link Nitrox2#PLANES} is the coarse tier's bits per dimension:
 * two for the default thermometer, one for the sign sketch under {@code -Divfaster.coarseBits=1}.
 * Every width here derives from {@code PLANES}, as {@link #encode} does when it sizes its landing
 * buffer, so a record stays correct at either setting.
 *
 * <p>Planes appear in threshold order, the order {@link Nitrox2#packPlanes} writes them, which is
 * what makes a record byte-identical to the on-disk coarse record and to a centroid's code. The
 * Hamming distance is additive over the concatenation, so one XOR and popcount over the whole
 * record scores it whatever {@code PLANES} is.
 *
 * <ol>
 *   <li>A document's planes are adjacent, so reading its code is one sequential run and the Hamming
 *       kernel scores the whole code in one pass.
 *   <li>The stride is 64 B aligned, so record {@code i} starts on a cache line. At the default two
 *       planes and dim=1024 a record is exactly 256 B, four lines with no padding.
 *   <li>One buffer holds every record, so the build allocates no per-document arrays.
 * </ol>
 *
 * @lucene.experimental
 */
final class DocPlanes {

  /** Cache line, and the record alignment. */
  static final int ALIGN = 64;

  private final byte[] buffer;
  private final int stride;
  private final int planeBytes;
  private final int count;

  private DocPlanes(byte[] buffer, int stride, int planeBytes, int count) {
    this.buffer = buffer;
    this.stride = stride;
    this.planeBytes = planeBytes;
    this.count = count;
  }

  /** Bytes per document record, a whole number of cache lines. */
  static int strideFor(int dim) {
    final int raw = Nitrox2.bytesPerVector(dim);
    return (raw + ALIGN - 1) / ALIGN * ALIGN;
  }

  /**
   * Encodes every document's planes once, in parallel over document ranges.
   *
   * <p>The in-heap source for clustering over arrays; the writer stages planes to a temp file
   * instead ({@link StagedVectors}), which is why this holds no donor-copy path. Every encode
   * writes straight into this worker's own records of the shared output buffer, since the ranges
   * are disjoint.
   *
   * @param rotated rotated document vectors, indexed by document
   */
  static DocPlanes encode(float[][] rotated, int count, int dim) throws IOException {
    final int planeBytes = Nitrox2.planeBytes(dim);
    final int stride = strideFor(dim);
    final byte[] buffer = new byte[Math.multiplyExact(count, stride)];
    // Hoisted out of the coordinate loop: the grid is a function of dim alone.
    final float clip = Nitrox2.clipFor(dim);
    final float invStep = Nitrox2.invStepFor(dim);

    Parallel.overRange(
        count,
        (from, to) -> {
          for (int i = from; i < to; i++) {
            encodeInto(rotated[i], dim, buffer, i * stride, planeBytes, clip, invStep);
          }
        });
    return new DocPlanes(buffer, stride, planeBytes, count);
  }

  /**
   * Encodes one vector's coarse code into {@code dest} at {@code off}: all {@link Nitrox2#PLANES}
   * thermometer planes concatenated in threshold order, through {@link Nitrox2#packPlanes}, the
   * sole encode primitive. That is the layout both the packed per-document record and the on-disk
   * coarse section use.
   */
  private static void encodeInto(
      float[] vector, int dim, byte[] dest, int off, int planeBytes, float clip, float invStep) {
    Nitrox2.packPlanes(vector, dim, dest, off, planeBytes);
  }

  /**
   * Copies document {@code i}'s whole coarse code into {@code dest}: {@code Nitrox2.PLANES *
   * planeBytes} sequential bytes from one cache-aligned record, based at zero, which is the
   * kernel's signature.
   */
  void copyInto(int i, byte[] dest) {
    System.arraycopy(buffer, i * stride, dest, 0, Nitrox2.PLANES * planeBytes);
  }

  /** The packed buffer; the writer emits the plane sections directly from it. */
  byte[] buffer() {
    return buffer;
  }

  /**
   * Byte offset of document {@code i}'s record, where its first plane begins. Plane {@code t} sits
   * at {@code offset(i) + t * planeBytes()}, for {@code t} in {@code [0, Nitrox2.PLANES)}.
   */
  int offset(int i) {
    return i * stride;
  }

  int stride() {
    return stride;
  }

  int planeBytes() {
    return planeBytes;
  }

  int count() {
    return count;
  }
}
