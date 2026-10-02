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

/**
 * The on-disk layout of one document's code-table record.
 *
 * <p>Payload first, header in the tail, record padded to a cache line:
 *
 * <pre>
 *   [ code: codeBytes, 64 B-ALIGNED ][ docId(4) ][ primaryCell(4) ][ 4 correction floats(16) ][ pad ]
 * </pre>
 *
 * <p>Ordering is what buys the alignment. A header-first record, {@code 8 + codeBytes + 16}, is 24
 * mod 64 at dim=1024, so records do not begin on cache lines and a code starting at offset 8 spans
 * 17 lines rather than 16: one extra line fetch per record on the rerank's scattered reads. With
 * the payload first the code starts at offset 0 and spans exactly 16 lines, and the 24-byte header
 * rides in the padding alignment would have wasted anyway, for 1088 B per record.
 *
 * <p>The corrections sit beside the header rather than before the code because the fine tier scores
 * from the code and reads the corrections only to finish the score, so the scoring loop touches one
 * contiguous run.
 *
 * <p>Every offset in this format comes from here, so a writer and a reader cannot derive it
 * differently.
 *
 * @lucene.experimental
 */
final class CodeRecord {

  /** Cache line, and the record alignment. */
  static final int ALIGN = 64;

  /** Bytes of header: docId and primaryCell. */
  static final int HEADER_BYTES = 8;

  /** Correction floats stored per record, whose meaning belongs to the fine quantizer. */
  static final int CORRECTIONS = 4;

  /** Bytes of corrections. */
  static final int CORRECTION_BYTES = CORRECTIONS * Float.BYTES;

  private CodeRecord() {}

  /** Total bytes per record: payload, header, corrections, padded up to a cache line. */
  static int length(int codeBytes) {
    return (codeBytes + HEADER_BYTES + CORRECTION_BYTES + ALIGN - 1) / ALIGN * ALIGN;
  }

  /**
   * Offset of the code payload: always zero, and 64 B-aligned as long as the record length is.
   * Named, so the fine tier's {@code codeOffset} argument is traceable to the layout.
   */
  static int codeOffset() {
    return 0;
  }

  /** Offset of the 4-byte document id. */
  static int docIdOffset(int codeBytes) {
    return codeBytes;
  }

  /** Offset of the 4-byte primary cell. */
  static int primaryCellOffset(int codeBytes) {
    return codeBytes + 4;
  }

  /**
   * Offset of the first correction float; correction {@code k} sits at {@code correctionBase + k *
   * Float.BYTES}. Split out so a reader caches it once per field and indexes the correction run by
   * arithmetic.
   */
  static int correctionBase(int codeBytes) {
    return codeBytes + HEADER_BYTES;
  }

  /** Offset of correction float {@code k}. */
  static int correctionOffset(int codeBytes, int k) {
    return correctionBase(codeBytes) + k * Float.BYTES;
  }

  /** Reads a little-endian int from a record. */
  static int readIntLE(byte[] rec, int off) {
    return (rec[off] & 0xFF)
        | ((rec[off + 1] & 0xFF) << 8)
        | ((rec[off + 2] & 0xFF) << 16)
        | ((rec[off + 3] & 0xFF) << 24);
  }

  /** Writes a little-endian int into a record. */
  static void writeIntLE(byte[] rec, int off, int v) {
    rec[off] = (byte) v;
    rec[off + 1] = (byte) (v >>> 8);
    rec[off + 2] = (byte) (v >>> 16);
    rec[off + 3] = (byte) (v >>> 24);
  }
}
