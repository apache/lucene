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
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the packed coarse-plane buffer.
 *
 * <p>The point of this class is that a document's planes are derived ONCE and every consumer reads
 * the same bytes. So the tests that matter are the two that would fail silently: that the packed
 * bytes agree with what {@link Nitrox2} would have produced (a divergence would mean routing and
 * the on-disk index disagree about a document's code, with no error anywhere), and that the record
 * stride keeps every document's code cache-line aligned.
 */
public class TestDocPlanes extends LuceneTestCase {

  /**
   * PACKED BYTES MUST EQUAL WHAT Nitrox2 PRODUCES, for every document.
   *
   * <p>This is the whole safety property of caching the planes. If the packed form drifted from the
   * canonical encoder, the routing scan would score against one code while the index stored another
   * (the writer and reader threshold mismatch that costs a silent recall loss) in a new place.
   */
  public void testPackedPlanesMatchNitrox2() throws IOException {
    for (int dim : new int[] {64, 128, 768, 1024}) {
      final int count = 200;
      final float[][] vectors = new float[count][];
      for (int i = 0; i < count; i++) {
        vectors[i] = randomVector(dim);
      }
      final DocPlanes planes = DocPlanes.encode(vectors, count, dim, null, null, null);
      final int cb = Nitrox2.bytesPerVector(dim);
      final byte[] exp = new byte[cb];
      final byte[] got = new byte[cb];
      for (int i = 0; i < count; i++) {
        Nitrox2.encode(vectors[i], dim, exp, 0);
        planes.copyInto(i, got);
        assertArrayEquals("coarse code must match Nitrox2 at dim=" + dim + " doc=" + i, exp, got);
      }
    }
  }

  /** Every document's record must begin on a cache line, which is why the stride is padded. */
  public void testStrideIsCacheLineAligned() {
    for (int dim : new int[] {64, 96, 128, 256, 768, 1024, 1536}) {
      final int stride = DocPlanes.strideFor(dim);
      assertEquals("stride must be 64-byte aligned at dim=" + dim, 0, stride % DocPlanes.ALIGN);
      final int raw = Nitrox2.bytesPerVector(dim);
      assertTrue("stride must hold both planes at dim=" + dim, stride >= raw);
      assertTrue(
          "stride must not waste a whole line at dim=" + dim, stride - raw < DocPlanes.ALIGN);
    }
    // At dim=1024 the planes are an exact multiple of the line size, with no padding.
    assertEquals(
        "dim=1024 is " + Nitrox2.PLANES + " x 128 B exactly",
        Nitrox2.PLANES * 128,
        DocPlanes.strideFor(1024));
  }

  /** Both planes of one document must be adjacent, since every consumer reads them together. */
  public void testPlanesOfOneDocumentAreAdjacent() throws IOException {
    final int dim = 256;
    final int count = 8;
    final float[][] vectors = new float[count][];
    for (int i = 0; i < count; i++) {
      vectors[i] = randomVector(dim);
    }
    final DocPlanes planes = DocPlanes.encode(vectors, count, dim, null, null, null);
    final int cb = Nitrox2.bytesPerVector(dim);
    final byte[] code = new byte[cb];
    final byte[] buf = planes.buffer();
    for (int i = 0; i < count; i++) {
      planes.copyInto(i, code);
      final int base = planes.offset(i);
      // The whole code (both planes concatenated) must be one contiguous run at the record offset.
      for (int b = 0; b < cb; b++) {
        assertEquals("code byte " + b + " of doc " + i, code[b], buf[base + b]);
      }
    }
    assertEquals("records must tile the buffer exactly", count * planes.stride(), buf.length);
  }

  /** A one-document buffer is the degenerate case and must still be well formed. */
  public void testSingleDocument() throws IOException {
    final int dim = 128;
    final float[][] v = {randomVector(dim)};
    final DocPlanes planes = DocPlanes.encode(v, 1, dim, null, null, null);
    assertEquals(1, planes.count());
    final int cb = Nitrox2.bytesPerVector(dim);
    final byte[] exp = new byte[cb];
    final byte[] got = new byte[cb];
    Nitrox2.encode(v[0], dim, exp, 0);
    planes.copyInto(0, got);
    assertArrayEquals(exp, got);
  }

  /**
   * Encoding must be parallel-safe: a corpus large enough to split across workers must give the
   * same bytes as one small enough to run single-threaded.
   *
   * <p>Workers write into disjoint ranges of ONE shared buffer, so a bounds error would corrupt a
   * neighbour's record rather than throw.
   */
  public void testParallelEncodeMatchesSerial() throws IOException {
    final int dim = 128;
    // Above Parallel's MIN_PER_THREAD, so this actually splits.
    final int count = 20000;
    final float[][] vectors = new float[count][];
    for (int i = 0; i < count; i++) {
      vectors[i] = randomVector(dim);
    }
    final DocPlanes planes = DocPlanes.encode(vectors, count, dim, null, null, null);
    final int cb = Nitrox2.bytesPerVector(dim);
    final byte[] exp = new byte[cb];
    final byte[] got = new byte[cb];
    for (int i = 0; i < count; i++) {
      Nitrox2.encode(vectors[i], dim, exp, 0);
      planes.copyInto(i, got);
      assertArrayEquals("coarse code of doc " + i + " under parallel encode", exp, got);
    }
  }

  private float[] randomVector(int dim) {
    final float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (random().nextGaussian() * std);
    }
    return v;
  }
}
