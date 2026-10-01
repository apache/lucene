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
package org.apache.lucene.analysis.morph;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;

public class TestTokenInfoFST extends LuceneTestCase {

  /** The kana window cached by the Japanese dictionary: 192 slots. */
  private static final int CACHE_FLOOR = 0x3040;

  private static final int CACHE_CEILING = 0x30FF;

  /** Number of terms, one per slot, so the cache window is filled completely. */
  private static final int CACHE_SLOTS = 1 + CACHE_CEILING - CACHE_FLOOR;

  /** CJK ideographs sit above the kana window, so terms starting here cache no root arc. */
  private static final int UNCACHED_FLOOR = 0x4E00;

  /**
   * {@link TokenInfoFST} is abstract only so that each language module can pin its own cache
   * window; it declares no abstract members, so a pass-through subclass is a faithful instance.
   */
  private static class SimpleTokenInfoFST extends TokenInfoFST {
    SimpleTokenInfoFST(FST<Long> fst, int cacheCeiling, int cacheFloor) throws IOException {
      super(fst, cacheCeiling, cacheFloor);
    }

    FST<Long> internalFST() {
      return fst;
    }
  }

  public void testCacheCeilingBelowFloorIsRejected() throws IOException {
    FST<Long> fst = buildFST(CACHE_FLOOR, 1, false);
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> new SimpleTokenInfoFST(fst, CACHE_FLOOR - 1, CACHE_FLOOR));
    assertTrue(e.getMessage().contains("cacheCeiling must be larger than cacheFloor"));
  }

  public void testCacheCeilingEqualToFloorIsAccepted() throws IOException {
    // Despite the message above, the check only rejects a ceiling strictly below the floor, so an
    // equal pair is legal and caches exactly one slot.
    SimpleTokenInfoFST fst =
        new SimpleTokenInfoFST(buildFST(CACHE_FLOOR, 2, false), CACHE_FLOOR, CACHE_FLOOR);
    Arc<Long> arc = fst.findTargetArc(CACHE_FLOOR, firstArc(fst), new Arc<>(), true, reader(fst));
    assertNotNull(arc);
    assertEquals(CACHE_FLOOR, arc.label());
  }

  public void testFindTargetArcCacheHitMatchesUncachedLookup() throws IOException {
    SimpleTokenInfoFST fst = build(CACHE_FLOOR, CACHE_SLOTS, false);

    Arc<Long> cached =
        fst.findTargetArc(CACHE_FLOOR, firstArc(fst), new Arc<>(), true, reader(fst));
    assertNotNull(cached);

    Arc<Long> direct =
        fst.findTargetArc(CACHE_FLOOR, firstArc(fst), new Arc<>(), false, reader(fst));
    assertNotNull(direct);

    assertEquals(direct.label(), cached.label());
    assertEquals(direct.output(), cached.output());
    assertEquals(direct.target(), cached.target());
  }

  public void testFindTargetArcCacheMissReturnsNull() throws IOException {
    // Only the first slot is populated, so another label inside the window must miss.
    SimpleTokenInfoFST fst = build(CACHE_FLOOR, 1, false);
    assertNull(fst.findTargetArc(CACHE_FLOOR + 1, firstArc(fst), new Arc<>(), true, reader(fst)));
  }

  public void testFindTargetArcBelowWindowDelegatesToFST() throws IOException {
    // ASCII labels sit below the kana floor. The floor guard has to keep them away from the cache
    // array, which they would index negatively.
    SimpleTokenInfoFST fst = build('a', 2, false);
    Arc<Long> arc = fst.findTargetArc('a', firstArc(fst), new Arc<>(), true, reader(fst));
    assertNotNull(arc);
    assertEquals('a', arc.label());
  }

  public void testFindTargetArcAboveWindowDelegatesToFST() throws IOException {
    // Nothing is cached, so the lookup has to fall through to the FST and still find the term.
    SimpleTokenInfoFST fst = build(UNCACHED_FLOOR, CACHE_SLOTS, false);
    Arc<Long> arc =
        fst.findTargetArc(UNCACHED_FLOOR, firstArc(fst), new Arc<>(), true, reader(fst));
    assertNotNull(arc);
    assertEquals(UNCACHED_FLOOR, arc.label());
  }

  public void testRamBytesUsedWithDistinctOutputs() throws IOException {
    // Ascending non-zero outputs: every cached arc carries a real output that has to be charged,
    // while the final output of each stays the shared no-output singleton.
    assertRamBytesUsed(build(CACHE_FLOOR, CACHE_SLOTS, false));
  }

  public void testRamBytesUsedWithSharedOutputs() throws IOException {
    // All terms share output 0, which is the no-output singleton, so both outputs of every cached
    // arc take the identity branch and none of them may be charged.
    assertRamBytesUsed(build(CACHE_FLOOR, CACHE_SLOTS, true));
  }

  public void testRamBytesUsedWithEmptyRootCache() throws IOException {
    // Nothing is cached, so only the fixed size plus the still fully allocated array is charged.
    assertRamBytesUsed(build(UNCACHED_FLOOR, CACHE_SLOTS, false));
  }

  public void testRamBytesUsedWithFinalOutputs() throws IOException {
    // The only fixture that charges nextFinalOutput: each leading character is both a term and the
    // prefix of a longer one, with the larger output on the shorter term, so the FST cannot push
    // the whole output down onto the root arc.
    assertRamBytesUsed(buildWithFinalOutputs(CACHE_FLOOR, CACHE_SLOTS));
  }

  /**
   * Substitutes {@link FST#ramBytesUsed()} for a reflective walk of the wrapped FST. This is
   * required rather than an optimization: a reflective walk of an FST does not agree with the size
   * the FST reports for itself (measured roughly 2x larger on these fixtures), so including it
   * would swamp the comparison. The wrapper reports the FST's own number verbatim, so substituting
   * it on both sides keeps the comparison about what the wrapper itself adds, which is still
   * measured reflectively.
   */
  private static final RamUsageTester.Accumulator FST_DELEGATING_ACCUMULATOR =
      new RamUsageTester.Accumulator() {
        @Override
        public long accumulateObject(
            Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
          if (o instanceof FST<?> fst) {
            return fst.ramBytesUsed();
          }
          return super.accumulateObject(o, shallowSize, fieldValues, queue);
        }
      };

  /**
   * Cross-checks the wrapper's own contribution against an independent reflective measurement,
   * within the 10% tolerance used for other {@code Accountable}s. Catches over- and under-counting
   * alike, which an inequality between two reported sizes cannot do.
   */
  private static void assertRamBytesUsed(SimpleTokenInfoFST fst) {
    long reported = reportedOverhead(fst);
    long actual =
        RamUsageTester.ramUsed(fst, FST_DELEGATING_ACCUMULATOR) - fst.internalFST().ramBytesUsed();
    assertEquals((double) actual, (double) reported, (double) actual * 0.10);
  }

  /** What the wrapper reports beyond the FST size it delegates to. */
  private static long reportedOverhead(SimpleTokenInfoFST fst) {
    return fst.ramBytesUsed() - fst.internalFST().ramBytesUsed();
  }

  private static Arc<Long> firstArc(TokenInfoFST fst) {
    return fst.getFirstArc(new Arc<>());
  }

  private static FST.BytesReader reader(TokenInfoFST fst) {
    return fst.getBytesReader();
  }

  /** Wraps {@link #buildFST} in the kana cache window used by most of these tests. */
  private static SimpleTokenInfoFST build(int firstChar, int count, boolean sharedOutputs)
      throws IOException {
    return new SimpleTokenInfoFST(
        buildFST(firstChar, count, sharedOutputs), CACHE_CEILING, CACHE_FLOOR);
  }

  /**
   * Builds a wrapper whose cached root arcs carry a non-empty final output. {@link
   * PositiveIntOutputs} does not require monotonic outputs, so giving the one-character term the
   * larger output leaves part of it on the arc's final output instead of on the arc itself.
   */
  private static SimpleTokenInfoFST buildWithFinalOutputs(int firstChar, int count)
      throws IOException {
    FSTCompiler<Long> fstCompiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE2, PositiveIntOutputs.getSingleton()).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (int i = 0; i < count; i++) {
      scratch.clear();
      scratch.append(firstChar + i);
      fstCompiler.add(scratch.get(), 1000L + i);
      scratch.append('a');
      fstCompiler.add(scratch.get(), 1L + i);
    }
    return new SimpleTokenInfoFST(
        FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader()),
        CACHE_CEILING,
        CACHE_FLOOR);
  }

  /**
   * Builds an FST holding {@code count} two-character terms, each with a distinct leading character
   * starting at {@code firstChar} so that every term claims its own root arc.
   */
  private static FST<Long> buildFST(int firstChar, int count, boolean sharedOutputs)
      throws IOException {
    FSTCompiler<Long> fstCompiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE2, PositiveIntOutputs.getSingleton()).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (int i = 0; i < count; i++) {
      scratch.clear();
      scratch.append(firstChar + i);
      scratch.append('a');
      fstCompiler.add(scratch.get(), sharedOutputs ? 0L : i + 1L);
    }
    return FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());
  }
}
