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

import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Pins the record layout's alignment property.
 *
 * <p>The whole point of putting the code payload first and the header in the tail is that every
 * code begins on a cache line. That is a property of the arithmetic, and arithmetic drifts silently
 * a change to the header size or the correction count would reintroduce the straddle that this
 * layout exists to remove, with no symptom other than slower scattered reads.
 */
public class TestCodeRecord extends LuceneTestCase {

  /** Every record length must be a whole number of cache lines, so record N starts on a line. */
  public void testRecordLengthIsCacheLineAligned() {
    for (int dim : new int[] {96, 128, 256, 512, 768, 1024, 1536}) {
      for (int bits : new int[] {1, 4, 8}) {
        final int codeBytes = bits * (dim / 8);
        final int len = CodeRecord.length(codeBytes);
        assertEquals(
            "record length must be a multiple of the cache line at dim=" + dim + " bits=" + bits,
            0,
            len % CodeRecord.ALIGN);
        assertTrue(
            "record must hold the payload, header and corrections",
            len >= codeBytes + CodeRecord.HEADER_BYTES + CodeRecord.CORRECTION_BYTES);
        assertTrue(
            "record must not waste a whole line at dim=" + dim,
            len - codeBytes - CodeRecord.HEADER_BYTES - CodeRecord.CORRECTION_BYTES
                < CodeRecord.ALIGN);
      }
    }
  }

  /** The code payload starts at zero, so a line-aligned record makes the code line-aligned. */
  public void testCodeStartsOnALine() {
    assertEquals("the payload leads the record", 0, CodeRecord.codeOffset());
    // At the production shape the code must span exactly codeBytes/64 lines, not one more.
    final int codeBytes = 1024;
    final int len = CodeRecord.length(codeBytes);
    assertEquals("dim=1024 b=8 record", 1088, len);
    final int linesSpanned =
        (CodeRecord.codeOffset() % CodeRecord.ALIGN + codeBytes + CodeRecord.ALIGN - 1)
            / CodeRecord.ALIGN;
    assertEquals("a 1024 B code must span exactly 16 lines, not 17", 16, linesSpanned);

    // The header-first layout, kept as the contrast this design is justified against.
    final int headerFirstOffset = 8;
    final int oldLines =
        (headerFirstOffset % CodeRecord.ALIGN + codeBytes + CodeRecord.ALIGN - 1)
            / CodeRecord.ALIGN;
    assertEquals("header-first would straddle into a 17th line", 17, oldLines);
  }

  /** Header and correction fields must not overlap each other or the payload. */
  public void testFieldsDoNotOverlap() {
    for (int codeBytes : new int[] {96, 256, 1024}) {
      final int len = CodeRecord.length(codeBytes);
      final int docId = CodeRecord.docIdOffset(codeBytes);
      final int cell = CodeRecord.primaryCellOffset(codeBytes);
      assertTrue("docId must follow the payload", docId >= codeBytes);
      assertEquals("primaryCell follows docId", docId + 4, cell);
      assertEquals(
          "corrections follow the header", cell + 4, CodeRecord.correctionOffset(codeBytes, 0));
      for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
        final int off = CodeRecord.correctionOffset(codeBytes, k);
        assertTrue("correction " + k + " must be inside the record", off + 4 <= len);
        assertTrue("correction " + k + " must follow the payload", off >= codeBytes);
      }
    }
  }

  /** Round-tripping every field must be exact and must not disturb its neighbours. */
  public void testFieldRoundTrip() {
    final int codeBytes = 256;
    final byte[] rec = new byte[CodeRecord.length(codeBytes)];
    // Fill the payload with a recognizable pattern, so a header write into it would be visible.
    for (int i = 0; i < codeBytes; i++) {
      rec[i] = (byte) (i * 31);
    }
    final int docId = 0x0BADF00D;
    final int cell = 0x00C0FFEE;
    CodeRecord.writeIntLE(rec, CodeRecord.docIdOffset(codeBytes), docId);
    CodeRecord.writeIntLE(rec, CodeRecord.primaryCellOffset(codeBytes), cell);
    final float[] corr = {1.5f, -2.25f, 1e-7f, 3.0e8f};
    for (int k = 0; k < corr.length; k++) {
      CodeRecord.writeIntLE(
          rec, CodeRecord.correctionOffset(codeBytes, k), Float.floatToIntBits(corr[k]));
    }

    assertEquals(docId, CodeRecord.readIntLE(rec, CodeRecord.docIdOffset(codeBytes)));
    assertEquals(cell, CodeRecord.readIntLE(rec, CodeRecord.primaryCellOffset(codeBytes)));
    for (int k = 0; k < corr.length; k++) {
      assertEquals(
          "correction " + k,
          corr[k],
          Float.intBitsToFloat(
              CodeRecord.readIntLE(rec, CodeRecord.correctionOffset(codeBytes, k))),
          0f);
    }
    // The payload must be untouched: a header written over the code is the failure this guards.
    for (int i = 0; i < codeBytes; i++) {
      assertEquals("payload byte " + i + " was overwritten", (byte) (i * 31), rec[i]);
    }
  }

  /** Negative and extreme ints must survive, since doc ids and cells are plain ints. */
  public void testIntEdgeCases() {
    final byte[] rec = new byte[CodeRecord.length(64)];
    for (int v : new int[] {0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, -12345}) {
      CodeRecord.writeIntLE(rec, CodeRecord.docIdOffset(64), v);
      assertEquals(v, CodeRecord.readIntLE(rec, CodeRecord.docIdOffset(64)));
    }
  }
}
