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
package org.apache.lucene.analysis.icu;

import static com.ibm.icu.text.UTF16.LEAD_SURROGATE_MAX_VALUE;
import static com.ibm.icu.text.UTF16.isLeadSurrogate;
import static com.ibm.icu.text.UTF16.isSurrogate;
import static com.ibm.icu.text.UTF16.isTrailSurrogate;

import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UTF16;
import java.io.IOException;
import java.io.Reader;
import java.util.function.IntBinaryOperator;

/**
 * An implementation of {@link Replaceable} that leverages {@link Replaceable}'s "metadata" concept
 * to integrally track (and correct) offset changes.
 *
 * <p>This decouples external invocation of {@link Transliterator#filteredTransliterate(Replaceable,
 * Transliterator.Position, boolean)} methods from granular tracking of offset diffs, which allows
 * composite Transliterators to be separated into constituent components, each with its own
 * dedicated section of the buffer (at any given point in time), without any compromise in terms of
 * the accuracy/granularity of offset diff tracking and correction.
 *
 * <p>This abstraction is a reasonably good fit, but is a little bit awkward at times because
 * offsets are relative, so offset context can't be trivially "copied" to different places within
 * the Replaceable buffer in the same way that, e.g., style/formatting data can. The awkwardness of
 * this mainly manifests in the need to jump through hoops to ensure parity between simple
 * "non-complex" String-replace operations and "complex" operations in which icu StringReplacer
 * copies buffer contents and context to the end of the buffer and manipulates it there before
 * copying it back into position and deleting. See {@link #complexCopyAsReplace(int, int)}, and
 * related comments, etc.
 */
class CircularReplaceable implements Replaceable {

  // These *FLOOR_CHAR_COUNT vars are temporarily set artificially low in order to evaluate
  // buffer growing. For working buffer, should be at least 2 because this affects the code
  // (avoids an initial check when copying the pre-context codepoint)
  private static final int FLOOR_CHAR_COUNT = 1;
  private static final int WORKING_BUFFER_FLOOR_CHAR_COUNT = 2;

  private static final int INITIAL_CAPACITY = Integer.highestOneBit(FLOOR_CHAR_COUNT) << 1;
  private static final int WORKING_BUFFER_INITIAL_CAPACITY =
      Integer.highestOneBit(WORKING_BUFFER_FLOOR_CHAR_COUNT) << 1;

  private int capacity;
  private char[] buf;
  int[] offsetCorrect; // leveraging Replaceable "metadata" feature
  private int mask;
  private int head = 0; // insertion point
  private int tail = 0; // last

  private boolean wbActive = false;
  private int watchKeyOriginalOffset = -1;
  private int watchKeyDeleteOffset = -1;
  private boolean detectKeyDelete = false;
  private final CircularReplaceable workingBuffer;

  private CircularReplaceable(boolean internal, int initialCapacity, boolean trackOffsets) {
    workingBuffer =
        internal
            ? null
            : new CircularReplaceable(true, WORKING_BUFFER_INITIAL_CAPACITY, trackOffsets);
    capacity = initialCapacity;
    buf = new char[capacity];
    mask = capacity - 1;
    if (trackOffsets) {
      offsetCorrect = new int[capacity];
    }
  }

  /**
   * Create new offset-correcting {@link CircularReplaceable} with a default initial capacity of
   * {@value #FLOOR_CHAR_COUNT}
   */
  public CircularReplaceable() {
    this(false, INITIAL_CAPACITY, true);
  }

  /**
   * Create new offset-correcting {@link CircularReplaceable} with the specified initial capacity.
   */
  public CircularReplaceable(int initialCapacity) {
    this(initialCapacity, true);
  }

  /**
   * Create new {@link CircularReplaceable} with a default initial capacity of {@value
   * #FLOOR_CHAR_COUNT}
   *
   * @param trackOffsets - true if this instance should track offset diffs
   */
  public CircularReplaceable(boolean trackOffsets) {
    this(false, INITIAL_CAPACITY, trackOffsets);
  }

  /**
   * Create new {@link CircularReplaceable}.
   *
   * @param initialCapacity - the initial capacity of the internal buffer
   * @param trackOffsets - true if this instance should track offset diffs
   */
  public CircularReplaceable(int initialCapacity, boolean trackOffsets) {
    this(
        false,
        Integer.highestOneBit(initialCapacity == 0 ? 1 : initialCapacity) << 1,
        trackOffsets);
  }

  /**
   * Shortcut ctor for creating a new {@link CircularReplaceable} with the internal buffer initially
   * populated by specified String value.
   *
   * @param initial - for initializing the contents of the internal buffer
   */
  public CircularReplaceable(String initial) {
    this(initial.length(), true);
    append(initial);
  }

  private void clear() {
    // NOTE: we're asserting that this should only ever be called on the workingBuffer (which
    // in turn has its own workingBuffer==null). The way the working buffer is used in practice,
    // we can't care about offsetCorrect, so we just ignore it (don't have to clear it).
    assert workingBuffer == null;
    head = 0;
    tail = 0;
    // Arrays.fill(offsetCorrect, 0);
  }

  int headDiff() {
    return offsetCorrect[head & mask];
  }

  /**
   * Called after the end of the input stream has been reached; if characters have been removed or
   * added after the end of the input stream, this method will register associated offset
   * corrections with the specified {@link OffsetCorrectionRegistrar} callback.
   *
   * @param registrar - offset diff callback
   */
  public void flushHeadDiff(OffsetCorrectionRegistrar registrar) {
    final int headMask = head & mask;
    if (offsetCorrect == null) {
      return;
    }
    final int headDiff = offsetCorrect[headMask];
    if (headDiff != 0) {
      registrar.register(head, headDiff);
      offsetCorrect[headMask] = 0;
    }
  }

  /**
   * Requests that up to `limit` characters be read from `in` and appended to this buffer. This
   * method is guaranteed to read at least one character (assuming one is available). This method
   * will cause the internal buffer to grow if necessary to append new content, but the buffer will
   * _not_ grow unless there is _no_ space left at initial invocation.
   *
   * @param in - input from which to read
   * @param limit - the maximum number of characters to read from `in`
   * @return - the number of characters read from the specified input Reader
   * @throws IOException - if an error reading from the specified input Reader
   */
  public int readFrom(Reader in, int limit) throws IOException {
    int spaceAavailable = mask - head + tail;
    if (spaceAavailable == 0) {
      // only reads as needed, so if we're being asked to read and have no space, we must
      // increase (double) capacity, which will set `spaceAvailable` equal to extant capacity
      spaceAavailable = capacity;
      ensureCapacity(capacity);
    }
    limit = Math.min(limit, spaceAavailable); // cap limit to space actually available
    int off = head & mask;
    final int straightCapacity = Math.min(spaceAavailable, capacity - off);
    int ret;
    if (limit <= straightCapacity) {
      // all requested chars can be served by one straight read
      ret = in.read(buf, off, limit);
      if (ret > 0) {
        head += ret;
      }
      return ret;
    }
    ret = in.read(buf, off, straightCapacity);
    if (ret < straightCapacity) {
      // blocked reading so just return what we have
      if (ret > 0) {
        head += ret;
      }
      return ret;
    }
    // wrap and try to read more
    final int secondRet = in.read(buf, 0, limit - ret);
    if (secondRet < 0) {
      head += ret;
      ret = ~ret;
    } else {
      ret += secondRet;
      head += ret;
    }
    return ret;
  }

  @Override
  public int length() {
    return wbActive ? (head + workingBuffer.length()) : head;
  }

  @Override
  public char charAt(int offset) {
    if (wbActive && offset >= head) {
      return workingBuffer.charAt(offset - head);
    }
    checkRangeSIOOBE(offset);
    return buf[offset & mask];
  }

  @Override
  public int char32At(int offset16) {
    if (wbActive && offset16 >= head) {
      return workingBuffer.char32At(offset16 - head);
    }
    checkRangeSIOOBE(offset16);

    // adapt UTF16.charAt(char[], int, int, int) for circular context
    char single = buf[offset16 & mask];
    if (!isSurrogate(single)) {
      return single;
    }
    // Convert the UTF-16 surrogate pair if necessary.
    // For simplicity in usage, and because the frequency of pairs is
    // low, look both directions.
    if (single <= LEAD_SURROGATE_MAX_VALUE) {
      offset16++;
      if (offset16 >= head) {
        return single;
      }
      char trail = buf[offset16 & mask];
      if (isTrailSurrogate(trail)) {
        return Character.toCodePoint(single, trail);
      }
    } else { // isTrailSurrogate(single), so
      if (offset16 == tail) {
        return single;
      }
      offset16--;
      char lead = buf[offset16 & mask];
      if (isLeadSurrogate(lead)) return Character.toCodePoint(lead, single);
    }
    return single; // return unmatched surrogate
  }

  @Override
  public void getChars(int srcStart, int srcLimit, char[] dst, int dstStart) {
    if (wbActive) {
      if (srcStart >= head) {
        workingBuffer.getChars(srcStart - head, srcLimit - head, dst, dstStart);
        return;
      } else if (srcLimit > head) {
        // handling this case is relatively complex; more importantly it should be completely
        // unnecessary, given the assumptions regarding use of the workingBuffer; so enforce
        // this assumption here.
        throw new IllegalArgumentException("invalid assumption about use of workingBuffer?");
      }
    }
    checkRangeSIOOBE(srcStart, srcLimit);
    _getChars(srcStart, srcLimit, dst, null, dstStart, 0);
  }

  private void _getChars(
      int srcStart,
      int srcLimit,
      char[] dst,
      int[] offsetDst,
      int dstStart,
      int headDiffIncrement) {
    srcStart &= mask;
    srcLimit &= mask;
    if (srcStart <= srcLimit) {
      final int len = srcLimit - srcStart;
      System.arraycopy(buf, srcStart, dst, dstStart, len);
      if (offsetDst != null) {
        // destPos always 0; because offsetCorrect is only ever shifted internally
        System.arraycopy(offsetCorrect, srcStart, offsetDst, 0, len + headDiffIncrement);
      }
    } else {
      final int len1 = capacity - srcStart;
      final int off2 = dstStart + len1;
      System.arraycopy(buf, srcStart, dst, dstStart, len1);
      System.arraycopy(buf, 0, dst, off2, srcLimit);
      if (offsetDst != null) {
        System.arraycopy(offsetCorrect, srcStart, offsetDst, 0, len1);
        System.arraycopy(offsetCorrect, 0, offsetDst, len1, srcLimit + headDiffIncrement);
      }
    }
  }

  @Override
  public String toString() {
    return toString(false);
  }

  String toString(boolean includeContext) {
    final int tailMask = (includeContext ? tail : tail + flushKeepContext) & mask;
    final int headMask = head & mask;
    StringBuilder sb;
    if (tailMask <= headMask) {
      if (!wbActive) {
        return new String(buf, tailMask, headMask - tailMask);
      } else {
        sb = new StringBuilder(head - tail + (workingBuffer.head - workingBuffer.tail));
        sb.append(buf, tailMask, headMask - tailMask);
      }
    } else {
      sb = new StringBuilder(head - tail + (workingBuffer.head - workingBuffer.tail));
      sb.append(buf, tailMask, buf.length - tailMask);
      sb.append(buf, 0, headMask);
      if (!wbActive) {
        return sb.toString();
      }
    }
    workingBuffer.appendTo(sb);
    return sb.toString();
  }

  private void appendTo(StringBuilder sb) {
    final int tailMask = (tail + flushKeepContext) & mask;
    final int headMask = head & mask;
    if (tailMask <= headMask) {
      sb.append(buf, tailMask, headMask - tailMask);
    } else {
      sb.append(buf, tailMask, buf.length - tailMask);
      sb.append(buf, 0, headMask);
    }
  }

  private void checkRangeSIOOBE(int start, int end) {
    if (start < tail || start > end || end > head) {
      throw new StringIndexOutOfBoundsException(
          "start " + start + ", end " + end + ", tail " + tail + ", head " + head);
    }
  }

  private void checkRangeSIOOBE(int idx) {
    if (idx < tail || idx >= head) {
      throw new StringIndexOutOfBoundsException("idx " + idx + ", tail " + tail + ", head " + head);
    }
  }

  /**
   * Append the contents of the specified char array to this buffer. The internal buffer will grow
   * as necessary to accommodate all of the requested append.
   *
   * <p>NOTE: in the case of offset-based metadata, it is crucial to distinguish this operation
   * (which does not introduce offset diff) from {@link #replace(int, int, char[], int, int)} at the
   * limit of the buffer (which _does_ introduce an offset diff).
   */
  public void append(char[] cbuf, int off, int len) {
    if (wbActive) {
      throw new IllegalStateException("tried to append input while workingBuffer is active");
    }
    final int newHead = head + len;
    ensureCapacity(newHead - tail);
    this._copy(head & mask, newHead & mask, cbuf, null, off, len, 0);
    head = newHead;
  }

  /**
   * Append the contents of the specified String to this buffer. The internal buffer will grow as
   * necessary to accommodate all of the requested append.
   *
   * <p>NOTE: in the case of offset-based metadata, it is crucial to distinguish this operation
   * (which does not introduce offset diff) from {@link #replace(int, int, String)} at the limit of
   * the buffer (which _does_ introduce an offset diff).
   */
  public void append(String s) {
    if (wbActive) {
      throw new IllegalStateException("tried to append input while workingBuffer is active");
    }
    append(s.toCharArray(), 0, s.length());
  }

  private boolean detectKeyDelete(int start, String text) {
    if (!detectKeyDelete) {
      return false;
    }
    detectKeyDelete = false;
    final int checkOffset = watchKeyDeleteOffset;
    watchKeyDeleteOffset = -1;
    return start == checkOffset && text.isEmpty();
  }

  /**
   * We flag this here because, despite the fact that common-prefix trimming is prescribed by the
   * `Replaceable` API docs for `replace(...)` methods, it serves no functional purpose for metadata
   * consisting of _offsets_ (as is the case in this class). The reason is that because of general
   * "start offset affinity", common _prefixes_ are guaranteed to inherit extant offsets anyway,
   * with or without special handling. We leave {@link #TRIM_PREFIXES} set to `true` here, because
   * it could in principle shortcircuit superfluous operations; but it's useful to have the
   * "functionally superfluous" nature of this optimization specifically called out here for the
   * sake of clarity.
   *
   * <p>NOTE: by contrast, common-suffix trimming _is_ functionally significant, because it can
   * change the basis for offset correction, on both removal _and_ insertion of characters,
   * potentially increasing the accuracy of offset resolution.
   */
  private static final boolean TRIM_PREFIXES = true;

  @Override
  public void replace(int start, int limit, String text) {
    // NOTE: detect keyDelete first because status is unconditionally cleared later in this method
    final boolean keyDelete = detectKeyDelete(start, text);
    if (wbActive) {
      if (start == head && text.isEmpty() && limit == head + workingBuffer.length()) {
        wbActive = false;
        detectKeyDelete = true;
        workingBuffer.clear();
        return;
      }
      watchKeyDeleteOffset = -1;
      if (start >= head) {
        workingBuffer.replace(start - head, limit - head, text);
        return;
      } else if (limit > head) {
        throw new IllegalArgumentException("invalid assumption about use of workingBuffer?");
      }
    } else if (text.length() == 1
        && '\uFFFF' == text.charAt(0)
        && start == head
        && limit == head
        && workingBuffer != null) {
      wbActive = true;
      workingBuffer.buf[0] = '\uFFFF';
      workingBuffer.head = 1;
      watchKeyDeleteOffset = -1;
      return;
    }
    watchKeyDeleteOffset = -1;
    if (limit > head) {
      limit = head;
    }
    checkRangeSIOOBE(start, limit);
    final int rawLimit = limit;
    int srcOff = 0;
    int srcLimit = text.length();
    final int origStart = start;
    while (TRIM_PREFIXES
        && start < limit
        && srcOff < srcLimit
        && buf[start & mask] == text.charAt(srcOff)) {
      start++;
      srcOff++;
    }
    int idx;
    int srcIdx;
    if (limit > start
        && srcLimit > srcOff
        && buf[(idx = limit - 1) & mask] == text.charAt(srcIdx = srcLimit - 1)) {
      do {
        idx--;
        srcIdx--;
      } while (idx >= start && srcIdx >= srcOff && buf[idx & mask] == text.charAt(srcIdx));
      limit = idx + 1;
      srcLimit = srcIdx + 1;
    }
    final boolean insert = origStart == limit;
    if (start < limit || srcOff < srcLimit) {
      // replace is not a no-op
      _replace(
          start, limit, text, srcOff, srcLimit, keyDelete, rawLimit, insert, srcOff >= srcLimit);
    }
  }

  private int collapseOffsets(int from, int to) {
    assert from <= to;
    int collapsed = to - from;
    for (int i = from; i < to; i++) {
      collapsed += offsetCorrect[i & mask]; // accumulate adjustments over the shift interval
    }
    return collapsed;
  }

  private void transferOffsets(int srcStart, int srcLimit, int destStart) {
    if (srcStart >= srcLimit) {
      return;
    }
    final int len = srcLimit - srcStart;
    srcStart &= mask;
    srcLimit &= mask;
    int destLimit = (destStart + len) & mask;
    destStart &= mask;
    if (srcStart <= srcLimit) {
      if (destStart <= destLimit) {
        // straight arraycopy
        System.arraycopy(offsetCorrect, srcStart, offsetCorrect, destStart, len);
      } else {
        final int len1 = capacity - destStart;
        System.arraycopy(offsetCorrect, srcStart, offsetCorrect, destStart, len1);
        System.arraycopy(offsetCorrect, srcStart + len1, offsetCorrect, 0, destLimit);
      }
    } else {
      // src wraps
      final int len1 = capacity - srcStart;
      System.arraycopy(offsetCorrect, srcStart, offsetCorrect, destStart, len1);
      if (destStart <= destLimit) {
        // dest does not wrap
        System.arraycopy(offsetCorrect, 0, offsetCorrect, destStart + len1, srcLimit);
      } else {
        // src and dest both wrap, but we know shift is always negative, which makes things simpler
        final int len2 = capacity - destStart - len1;
        System.arraycopy(offsetCorrect, 0, offsetCorrect, destStart + len1, len2);
        System.arraycopy(offsetCorrect, len2, offsetCorrect, 0, srcLimit - len2);
      }
    }
  }

  /**
   * This handles a special case, where we compensate for an immediately preceding complementary
   * "copy" insertion that (together with this delete) is conceptually a single "replace" operation.
   *
   * <pre>
   *   2> copy acadae/1/2/6 => acadaec
   *   2>   StringReplacer.replace(StringReplacer.java:159) (copy lead context codepoint)
   *   2>         OR replace acadae/6/6/\uFFFF        :162) (copy initial lead pseudo-codepoint)
   *   2>   StringReplacer.replace(StringReplacer.java:159) (copy lead context codepoint)
   *   2> copy acadaec/3/4/7 => acadaecd
   *   2>   StringReplacer.replace(StringReplacer.java:187) (copy trail context codepoint)
   *   2> replace acadaecd/7/7/b => acadaecbd
   *   2>       rule filters from original source offset to dest by means of "replace". This
   *   2>       carries no offset metadata, but we're no worse off than if this were executed
   *   2>       directly via in-place "replace".
   *   2>   StringReplacer.replace(StringReplacer.java:212)
   *   2> copy acadaecbd/7/8/2 => acbadaecbd
   *   2>   StringReplacer.replace(StringReplacer.java:223) (copy from tmp buf to inline offset)
   *   2> replace acbadaecbd/7/10/ => acbadae
   *   2>   StringReplacer.replace(StringReplacer.java:224) (delete temp buffer)
   *   2> replace acbadae/3/4/ => acbdae
   *   2>   StringReplacer.replace(StringReplacer.java:227) (delete original key)
   * </pre>
   *
   * The above sequence can be easily and unambiguously recognized in its entirety as distinct from
   * any other type of manipulation, and handled as a special case that avoids needless shifting
   * and, more importantly, preserves integrity of headDiff without jumping through a bunch of
   * convoluted hoops.
   *
   * <p>As explained in `StringReplacer.replace(...)`, in most cases, this more complex sequence is
   * only executed the first time a transliteration rule is encountered (StringReplacer instances
   * are static and reused); Subsequent executions in many (most?) cases use inline "replace".
   * Unless handled carefully, this situation has the potential to result in inconsistent (and
   * stateful!) behavior out of an ostensibly stateless component.
   */
  private void complexCopyAsReplace(int srcStart, int srcLimit) {
    int checkSrcIdx;
    int checkDestIdx;
    final int suffixSrcFloor;
    final int suffixDstFloor;
    if (!TRIM_PREFIXES) {
      suffixSrcFloor = srcStart;
      suffixDstFloor = watchKeyOriginalOffset;
    } else {
      // prefixes must be recognized first in order to mark them as ineligible for suffix matching
      checkSrcIdx = srcStart;
      checkDestIdx = watchKeyOriginalOffset;
      while (checkSrcIdx < srcLimit
          && checkDestIdx < srcStart
          && buf[checkSrcIdx & mask] == buf[checkDestIdx & mask]) {
        checkSrcIdx++;
        checkDestIdx++;
      }
      suffixSrcFloor = checkSrcIdx;
      suffixDstFloor = checkDestIdx;
    }
    checkSrcIdx = srcLimit - 1;
    checkDestIdx = srcStart - 1;
    final int trimmedLimit;
    if (checkDestIdx < suffixDstFloor
        || checkSrcIdx < suffixSrcFloor
        || buf[checkSrcIdx & mask] != buf[checkDestIdx & mask]) {
      // no common suffix
      checkSrcIdx++;
      checkDestIdx++;
      trimmedLimit = srcLimit;
    } else {
      do {
        // keep advancing until common suffixes diverge
        checkDestIdx--;
        checkSrcIdx--;
      } while (checkDestIdx >= suffixDstFloor
          && checkSrcIdx >= suffixSrcFloor
          && buf[checkSrcIdx & mask] == buf[checkDestIdx & mask]);
      if (checkSrcIdx + 1 == srcStart) {
        // source in its entirety is (the same as or) a suffix of replacement.
        // the offset of the first char in the dest range should already hold the original offset of
        // the source key, so don't overwrite it! We increment `checkSrcIdx` and `checkDestIdx` in
        // order to re-associate the remaining suffix offsets with their corresponding
        // "replacements"; the remainder of the offsets in the "dest" (replacement) range should
        // already hold `-1` from the "insert" operation, so we're done.
        checkSrcIdx += 2; // 1 to get to suffix start, 1 to not overwrite original source offset
        checkDestIdx += 2;
        trimmedLimit = checkSrcIdx - 1;
      } else if (checkDestIdx + 1 == watchKeyOriginalOffset) {
        // replacement in its entirety is a suffix of source
        // as above (and for the same reason), we increment `checkSrcIdx` and `checkDestIdx` ...
        checkSrcIdx += 2; // 1 to get to suffix start, 1 to not overwrite original source offset
        checkDestIdx += 2;
        // ... but we also set `rawLimit` to `watchKeyOriginalOffset`, in order to apply accumulated
        // `externalAdjust` (from the `retroShift < 0` block below) to the beginning of the shared
        // suffix range
        trimmedLimit = checkSrcIdx - 1;
      } else {
        checkDestIdx++;
        trimmedLimit = ++checkSrcIdx;
      }
    }
    final int srcLen = srcLimit - srcStart;
    final int replacementSize = srcStart - watchKeyOriginalOffset;
    final int retroShift = replacementSize - srcLen;
    int collapsedOffsets = 0;
    if (retroShift < 0) {
      // "source key" range is larger than the dest ("replacement") range, so we preserve as
      // many the original offsets as possible (up to `preserveOffsetsLimit`); the remainder
      // of the offsets are accumulated into `externalAdjust`, to be applied after the current
      // ("non-retro") shift completes.
      final int preserveOffsetsLimit = trimmedLimit + retroShift;
      if (preserveOffsetsLimit == srcStart && srcStart > watchKeyOriginalOffset) {
        collapsedOffsets = collapseOffsets(preserveOffsetsLimit + 1, trimmedLimit + 1);
      } else {
        collapsedOffsets = collapseOffsets(preserveOffsetsLimit, trimmedLimit);
      }
      transferOffsets(srcStart + 1, preserveOffsetsLimit, watchKeyOriginalOffset + 1);
    } else if (trimmedLimit > srcStart) {
      // "source key" range is not larger than the dest ("replacement") range, so we copy all
      // offsets from the "source key" range directly into the beginning of the "replacement"
      // range; the remainder of "replacement" range offsets should be fine.
      transferOffsets(srcStart + 1, trimmedLimit, watchKeyOriginalOffset + 1);
    }
    transferOffsets(checkSrcIdx, head + 1, checkDestIdx);
    if (collapsedOffsets > 0) {
      int i = Math.max(srcStart - (srcLimit - trimmedLimit), watchKeyOriginalOffset + 1);
      // NOTE: see below. Arguably the "copy" approach is more appropriate, because after the first
      // replace, "A" inherits from "e", and "Z" points to the same position as "A". After the
      // second replace, "A" inherits from "c", and "Z" still points to the same position as "A" --
      // i.e., `-1` offset correction
      // 2> Stage1: abcde => abcdAZYX (AZYX)
      // 2> Stage2: abcdAZYX => aBAZYX (BAZ)
      // 2> mid-r:[0, 0, 0, 0, 0, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0]
      // 2> mid-c:[0, 0, 0, 0, 0, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0]
      // 2> end-r:[0, 0, 0, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
      // 2> end-c:[0, 0, 0, -1, 1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
      int iMask = i & mask;
      if (i < srcStart && offsetCorrect[iMask] == -1) {
        do {
          offsetCorrect[iMask] = 0;
        } while (--collapsedOffsets > 0
            && ++i < srcStart
            && offsetCorrect[iMask = (i & mask)] == -1);
      }
      if (collapsedOffsets > 0) {
        offsetCorrect[srcStart & mask] += collapsedOffsets;
      }
    }
    for (int i = head - srcLen + 1; i <= head; i++) {
      offsetCorrect[i & mask] = 0; // clear orphaned metadata
    }
  }

  static boolean DETECT_INTRODUCED_UNPAIRED_SURROGATES = true;
  static boolean FIX_INTRODUCED_UNPAIRED_SURROGATES = true;

  private void _replace(
      int start,
      int limit,
      String text,
      int srcStart,
      int srcLimit,
      boolean keyDelete,
      int rawLimit,
      boolean insert,
      boolean deletePrefix) {
    final int srcLen = srcLimit - srcStart;
    final int destLen = limit - start;
    int externalAdjust = 0;
    if (destLen != srcLen) {
      final int origLimit = limit;
      final int shift = srcLen - destLen;
      limit += shift;
      int restore = Integer.MIN_VALUE;
      if (!keyDelete) {
        // the normal case; we accumulate offsets over the shift interval in order to apply them
        // externally
        // at the new offset
        if (shift < 0 && offsetCorrect != null) {
          if (rawLimit == origLimit) {
            externalAdjust = collapseOffsets(limit, origLimit);
          } else {
            externalAdjust = collapseOffsets(limit + 1, origLimit + 1);
            assert rawLimit > origLimit;
            restore = offsetCorrect[limit & mask];
          }
        }
        shift(origLimit, shift, restore, externalAdjust, insert, rawLimit, deletePrefix);
      } else {
        assert shift < 0;
        assert text.isEmpty();
        assert origLimit == rawLimit;
        if (DETECT_INTRODUCED_UNPAIRED_SURROGATES) {
          // Not 100% certain of this diagnosis, but quick dump of my current thinking:
          // Because complex submatchers process 16-bit codepoints, but matchrules (esp. _negative_
          // matchrules) can overreport their anteContext limit to accommodate 16- and 32-bit
          // codepoints, it is possible for some matchrules to match too much antecontext; this
          // is wrong, but relatively harmless unless in the case of a negative matchrule that
          // matches on an unmatched surrogate boundary.

          // We can mitigate this here, but I think it's an ICU problem. N.b., I think this only
          // affects the _complex_ copy/replace/workingBuffer case. Assume this was introduced
          // erroneously, and remove it. In practice this should happen vanishingly infrequently.

          // Unfortunately, we can't fix this by removing the offending character and shifting the
          // contents of the buffer, because that would mess up ICU code position accounting. So
          // we'll replace the offending character with the Unicode "replacement char"

          // NOTE: this code could very likely be optimized to only the cases that actually get
          // triggered in practice (assuming that a full scan of the replacement may _not_ be
          // necessary). But if fixed upstream in ICU4J, this entire block could just be removed,
          // which would be preferable.

          int i = Math.max(watchKeyOriginalOffset - 1, 0);
          if (i < start - 1) {
            if (!UTF16.isLeadSurrogate(charAt(i))) {
              // non-surrogate, or trail surrogate presumably paired with out-of-range lead
              // surrogate
              i++;
            }
            while (i < start - 1) {
              char c = charAt(i);
              if (!UTF16.isSurrogate(c)) {
                // common case; simply advance
                i++;
              } else if (UTF16.isLeadSurrogate(c)) {
                assert Character.isHighSurrogate(c);
                // found lead (high) surrogate
                c = charAt(++i);
                if (!UTF16.isTrailSurrogate(c)) {
                  assert false : "we have not encountered this case in practice";
                  assert (introducedUnpairedSurrogate =
                          "found1 a lone (lead/high)! wkoo="
                              + watchKeyOriginalOffset
                              + ", i="
                              + (i - 1)
                              + ", start="
                              + start)
                      != null;
                  if (FIX_INTRODUCED_UNPAIRED_SURROGATES) {
                    buf[(i - 1) & mask] = '\ufffd';
                  }
                }
              } else {
                assert Character.isLowSurrogate(c);
                // found trail (low) surrogate
                // because of the direction of processing, the only way we get here is
                // for an unpaired trail surrogate; so correct it
                // Hiragana-Latin, wkoo=460, i=460, start=462
                assert (introducedUnpairedSurrogate =
                        "found1 a lone (trail/low)! wkoo="
                            + watchKeyOriginalOffset
                            + ", i="
                            + i
                            + ", start="
                            + start)
                    != null;
                if (FIX_INTRODUCED_UNPAIRED_SURROGATES) {
                  buf[i & mask] = '\ufffd';
                }
                i++;
              }
            }
          }
          if (i == start) {
            assert start == 0;
          } else {
            assert i == start - 1;
            if (UTF16.isLeadSurrogate(charAt(i))) {
              // check that it does have a trail
              if (!UTF16.isTrailSurrogate(charAt(origLimit))) {
                // am-chr wkoo=0, i=0, start=0
                // Katakana-Latin wkoo=164, i=164, start=165
                // Kana-Latn, wkoo=239, i=239, start=240
                // Hira-Latn, wkoo=44, i=44, start=45, origLimit=46
                // Hiragana-Latin, wkoo=840, i=840, start=841, origLimit=842
                assert (introducedUnpairedSurrogate =
                        "found2 a lone (lead/high)! wkoo="
                            + watchKeyOriginalOffset
                            + ", i="
                            + i
                            + ", start="
                            + start
                            + ", origLimit="
                            + origLimit)
                    != null;
                if (FIX_INTRODUCED_UNPAIRED_SURROGATES) {
                  buf[i & mask] = '\ufffd';
                }
              }
            } else if (origLimit < head) {
              // check that it doesn't have a trail
              if (UTF16.isTrailSurrogate(charAt(origLimit))) {
                assert false : "we have not encountered this case in practice";
                assert (introducedUnpairedSurrogate =
                        "found2 a lone (trail/low)! wkoo="
                            + watchKeyOriginalOffset
                            + ", i="
                            + origLimit
                            + ", start="
                            + start)
                    != null;
                if (FIX_INTRODUCED_UNPAIRED_SURROGATES) {
                  buf[origLimit & mask] = '\ufffd';
                }
              }
            }
          }
        }
        if (offsetCorrect != null) {
          complexCopyAsReplace(start, origLimit);
        }
        final int[] tmpOffsetCorrect = offsetCorrect;
        this.offsetCorrect = null;
        // NOTE: because `offsetCorrect` manipulation is disabled, many of the below args are
        // irrelevant
        shift(origLimit, shift, 0, 0, insert, rawLimit, false);
        this.offsetCorrect = tmpOffsetCorrect;
      }
    }
    // we have the appropriate amount of space now
    start &= mask;
    limit &= mask;
    if (start <= limit) {
      text.getChars(srcStart, srcLimit, buf, start);
    } else {
      final int wrap = srcStart + capacity - start;
      text.getChars(srcStart, wrap, buf, start);
      text.getChars(wrap, srcLimit, buf, 0);
    }
  }

  private static String introducedUnpairedSurrogate;
  /**
   * get and clear the message associated with the rare event of an introduced unpaired surrogate.
   */
  static String introducedUnpairedSurrogate() {
    final String ret = introducedUnpairedSurrogate;
    introducedUnpairedSurrogate = null;
    return ret;
  }

  @Override
  public void replace(int start, int limit, char[] chars, int charsStart, int charsLen) {
    if (wbActive) {
      if (start >= head) {
        workingBuffer.replace(start - head, limit - head, chars, charsStart, charsLen);
        return;
      } else if (limit > head) {
        throw new IllegalArgumentException("invalid assumption about use of workingBuffer?");
      }
    }
    if (limit > head) {
      limit = head;
    }
    checkRangeSIOOBE(start, limit);
    final int rawLimit = limit;
    final int origStart = start;
    final int charsLimit = charsStart + charsLen;
    while (TRIM_PREFIXES
        && start < limit
        && charsStart < charsLimit
        && buf[start & mask] == chars[charsStart]) {
      start++;
      charsStart++;
      charsLen--;
    }
    int idx;
    int charsIdx;
    if (limit > start
        && charsLimit > charsStart
        && buf[(idx = limit - 1) & mask] == chars[charsIdx = charsLimit - 1]) {
      do {
        idx--;
        charsIdx--;
        charsLen--;
      } while (idx >= start && charsIdx >= charsStart && buf[idx & mask] == chars[charsIdx]);
      limit = idx + 1;
    }
    final boolean insert = origStart == limit;
    if (start < limit || charsLen != 0) {
      // replace is not a no-op
      _replace(start, limit, chars, charsStart, charsLen, rawLimit, insert, charsLen == 0);
    }
  }

  private void _replace(
      int start,
      int limit,
      char[] chars,
      int charsStart,
      int charsLen,
      int rawLimit,
      boolean insert,
      boolean deletePrefix) {
    final int destLen = limit - start;
    int externalAdjust = 0;
    if (destLen != charsLen) {
      final int origLimit = limit;
      final int shift = charsLen - destLen;
      limit += shift;
      int restore = Integer.MIN_VALUE;
      if (shift < 0 && offsetCorrect != null) {
        if (rawLimit == origLimit) {
          externalAdjust = collapseOffsets(limit, origLimit);
        } else {
          assert rawLimit > origLimit;
          externalAdjust = collapseOffsets(limit + 1, origLimit + 1);
          restore = offsetCorrect[limit & mask];
        }
      }
      shift(origLimit, shift, restore, externalAdjust, insert, rawLimit, deletePrefix);
    }
    _copy(start & mask, limit & mask, chars, null, charsStart, charsLen, 0);
  }

  private void _copy(
      int start,
      int limit,
      char[] chars,
      int[] offsetSrc,
      int charsStart,
      int charsLen,
      int headDiffIncrement) {
    // we have the appropriate amount of space now
    if (start <= limit) {
      System.arraycopy(chars, charsStart, buf, start, charsLen);
      if (offsetSrc != null) {
        System.arraycopy(offsetSrc, 0, offsetCorrect, start, charsLen + headDiffIncrement);
      }
    } else {
      final int len1 = capacity - start;
      final int off2 = charsStart + len1;
      System.arraycopy(chars, charsStart, buf, start, len1);
      System.arraycopy(chars, off2, buf, 0, limit);
      if (offsetSrc != null) {
        System.arraycopy(offsetSrc, 0, offsetCorrect, start, len1);
        System.arraycopy(offsetSrc, len1, offsetCorrect, 0, limit + headDiffIncrement);
      }
    }
  }

  static final class OffsetCorrectionRegistrar {
    private int cumuDiff;
    private final IntBinaryOperator register;

    OffsetCorrectionRegistrar(IntBinaryOperator register) {
      this.register = register;
    }

    void register(int offset, int diff) {
      assert diff != 0;
      cumuDiff += diff;
      register.applyAsInt(offset, cumuDiff);
    }
  }

  private static final int FLUSH_KEEP_CONTEXT = 1;
  private int flushKeepContext = 0;

  /**
   * As {@link #flush(char[], int, int, int, int, OffsetCorrectionRegistrar)}, with the default
   * `keepContext` of {@value #FLUSH_KEEP_CONTEXT}.
   */
  public int flush(
      char[] dst, int dstStart, int dstLen, int toOffset, OffsetCorrectionRegistrar registrar) {
    return _flush(dst, dstStart, dstLen, toOffset, FLUSH_KEEP_CONTEXT, registrar);
  }

  /**
   * Flush the contents of this buffer to the specified char[] destination (up to specified
   * `toOffset`, if destination space allows). Associated offset corrections will be flushed to the
   * specified `registrar` callback. `keepContext` specifies the amount of context that should,
   * although flushed to the output dest, be kept in the buffer to serve as antecontext for
   * subsequent transliteration windows.
   *
   * @param dst - destination to receive content
   * @param dstStart - start offset for destination content
   * @param dstLen - max number of characters to be flushed to dest
   * @param toOffset - source limit offset (exclusive) that should be flushed to dest
   * @param keepContext - amount of antecontext to retain in buffer to serve as antecontext for
   *     subsequent transliteration windows
   * @param registrar - callback to receive notifications of offset diffs
   * @return - the number of characters flushed to dest
   */
  public int flush(
      char[] dst,
      int dstStart,
      int dstLen,
      int toOffset,
      int keepContext,
      OffsetCorrectionRegistrar registrar) {
    return _flush(
        dst, dstStart, dstLen, toOffset, Math.max(keepContext, FLUSH_KEEP_CONTEXT), registrar);
  }

  private int _flush(
      char[] dst,
      int dstStart,
      int dstLen,
      int toOffset,
      int keepContext,
      OffsetCorrectionRegistrar registrar) {
    assert !wbActive;

    // for output purposes, tail includes extant flushKeepContext
    final int oldTail = tail + flushKeepContext;

    if (toOffset <= oldTail) {
      return 0;
    }
    final int newTail = Math.min(toOffset, oldTail + dstLen);
    if (newTail > head) {
      // we could be more lenient here, but there's not reason that callers can't assume
      // responsibility for passing an appropriate `len` param
      throw new StringIndexOutOfBoundsException(
          "toOffset " + toOffset + ", tail " + tail + ", head " + head);
    }
    getChars(oldTail, newTail, dst, dstStart);
    if (newTail - keepContext < tail) {
      flushKeepContext = newTail - tail;
    } else {
      flushKeepContext = keepContext;
    }
    tail = newTail - flushKeepContext;
    if (offsetCorrect == null) {
      return newTail - oldTail;
    }
    for (int i = oldTail; i < newTail; i++) {
      final int iMask = i & mask;
      final int diff = offsetCorrect[iMask];
      if (diff != 0) {
        registrar.register(i, diff);
        offsetCorrect[iMask] = 0; // clear offset metadata
      }
    }
    return newTail - oldTail;
  }

  /**
   * Note: `copy` cannot actually preserve the offset metadata that we're interested in, because of
   * the way ICU API conceives of "metadata". This may lose us some precision, but it does simplify
   * things. No way around this one.
   */
  @Override
  public void copy(int start, int limit, int dest) {
    final int len = limit - start;
    if (wbActive) {
      if (start >= head) {
        if (dest <= head) {
          // normal "copy from wb to main" case
          // NOTE: wb never gets flushed, so it's always a straight arraycopy from source
          _replace(dest, dest, workingBuffer.buf, start - head, len, dest, true, false);
          watchKeyOriginalOffset = dest;
          watchKeyDeleteOffset = dest + len;
        } else {
          // copy within wb ... unusual?
          // NOTE: this can sometimes happen when the lead context codepoint is copied
          // _as the trailing context codepoint_ -- weird (to the point of being possibly
          // unintentional), but that's actually what StringReplacer does, so we must
          // support it.
          workingBuffer.copy(start - head, limit - head, dest - head);
        }
      } else if (limit > head) {
        throw new IllegalArgumentException("invalid assumption about use of workingBuffer?");
      } else if (dest > head) {
        // normal "copy from main to wb" case
        final int wbDest = dest - head;
        workingBuffer.shift(wbDest, len, 0, 0, true, wbDest, false);
        _getChars(start, limit, workingBuffer.buf, null, wbDest, 0);
      }
      return;
    } else if (dest == head && len <= 2 && workingBuffer != null) {
      // initial context codepoint copy; transition into "workingBuffer" state
      wbActive = true;
      _getChars(start, limit, workingBuffer.buf, null, 0, 0);
      workingBuffer.head = len;
      return;
    }
    checkRangeSIOOBE(start, limit);
    if (dest < tail || dest > head) {
      throw new StringIndexOutOfBoundsException(
          "dest " + dest + ", tail " + tail + ", head " + head);
    }
    // NOTE: `externalOffsetAdjust` is only for negative shift; `copy` is always
    // positive shift, so `externalOffsetAdjust=0`
    shift(dest, len, 0, 0, true, dest, false);
    if (start >= dest) {
      start += len;
      limit += len;
    }
    final int srcStart = start & mask;
    final int srcLimit = limit & mask;
    final int dstStart = dest & mask;
    final int dstLimit = (dest + len) & mask;
    if (srcStart <= srcLimit) {
      if (dstStart <= dstLimit) {
        // simple case, straight copy
        System.arraycopy(buf, srcStart, buf, dstStart, len);
      } else {
        final int len1 = capacity - dstStart;
        final int off2 = srcStart + len1;
        System.arraycopy(buf, srcStart, buf, dstStart, len1);
        System.arraycopy(buf, off2, buf, 0, dstLimit);
      }
    } else {
      assert dstStart <= dstLimit; // src and dst don't overlap, so only one can wrap
      final int len1 = capacity - srcStart;
      final int off2 = dstStart + len1;
      System.arraycopy(buf, srcStart, buf, dstStart, len1);
      System.arraycopy(buf, 0, buf, off2, srcLimit);
    }
  }

  private int correctOffset(
      final int newIdx,
      int restoreShiftDest,
      int externalOffsetAdjust,
      int shift,
      boolean insert,
      int newHead,
      int eoAdjustIdx,
      boolean deletePrefix) {
    if (offsetCorrect == null) {
      head = newHead;
      return newIdx;
    }
    if (restoreShiftDest != Integer.MIN_VALUE) {
      offsetCorrect[newIdx & mask] = restoreShiftDest;
    }
    if (shift < 0) {
      if (externalOffsetAdjust != 0) {
        // because of the way negative shift offsets are accumulated, eoAdjust can never be negative
        assert externalOffsetAdjust > 0;
        final int shiftedEoAdjustIdx = eoAdjustIdx + shift;
        int i = deletePrefix ? newIdx + 1 : newIdx;
        int iMask;
        if (i < shiftedEoAdjustIdx && offsetCorrect[iMask = (i & mask)] == -1) {
          // as long as we encounter a run of "inserted" characters, we first try to allow them to
          // "inherit" offsets from preceding adjustments. It's tempting to view this as weird and
          // "special-casey", but in fact it's well-founded: inserted chars are defined as
          // "inserted" relative to the preceding run of characters. When a chain of inserted
          // characters follows a preceding run that has had characters _removed_, it should as far
          // as possible inherit offsets from the removed characters, rather than "jump" the removal
          // after the inserted chars. "As far as possible" here means that once we encounter a
          // character that is _not_ inserted (i.e., `offset != -1`), that character should be
          // considered a boundary across which contiguous offsets should _not_ be inherited.
          do {
            offsetCorrect[iMask] = 0;
          } while (--externalOffsetAdjust > 0
              && ++i < shiftedEoAdjustIdx
              && offsetCorrect[iMask = (i & mask)] == -1);
        }
        // any remaining adjustments are incorporated at eoAdjustIdx
        offsetCorrect[shiftedEoAdjustIdx & mask] += externalOffsetAdjust;
      }
      for (int i = head; i > newHead; i--) {
        // clear any orphaned metadata
        offsetCorrect[i & mask] = 0;
      }
    } else {
      final int maxOffsetCorrectIdx = insert ? newIdx : newIdx - 1;
      final int floor = maxOffsetCorrectIdx - shift;
      for (int i = maxOffsetCorrectIdx; i > floor; i--) {
        offsetCorrect[i & mask] = -1;
      }
    }
    head = newHead;
    return newIdx;
  }

  private int shift(
      int origIdx,
      int shift,
      int restoreShiftDest,
      int externalOffsetAdjust,
      boolean insert,
      int eoAdjustIdx,
      boolean deletePrefix) {
    final int newHead = head + shift;
    if (shift > 0) {
      ensureCapacity(newHead - tail);
    }
    final int newIdx = origIdx + shift;
    final int origIdxMask = origIdx & mask;
    final int newIdxMask = newIdx & mask;
    final int origHeadMask = head & mask;
    final int newHeadMask = newHead & mask;

    final int len = head - origIdx;
    final int remainder;
    final int headDiffIncrement;
    if (shift > 0) {
      if (origIdxMask <= origHeadMask && newIdx <= newHeadMask) {
        // how much can we directly copy without overwriting our own source?
        final int straightCopy = Math.min(shift, len);
        final int srcOff = origHeadMask - straightCopy;
        final int dstOff = newHeadMask - straightCopy;
        System.arraycopy(buf, srcOff, buf, dstOff, straightCopy);
        if (offsetCorrect != null) {
          System.arraycopy(offsetCorrect, srcOff, offsetCorrect, dstOff, straightCopy + 1);
        }
        if (len == straightCopy) {
          // ranges did not overlap
          return correctOffset(
              newIdx,
              restoreShiftDest,
              externalOffsetAdjust,
              shift,
              insert,
              newHead,
              eoAdjustIdx,
              deletePrefix);
        }
        remainder = len - straightCopy;
        headDiffIncrement = 0;
      } else {
        remainder = len;
        headDiffIncrement = 1;
      }
    } else if (origIdxMask <= origHeadMask && newIdx <= newHeadMask) {
      // we can optimize this trivial case with a straight array copy
      System.arraycopy(buf, origIdxMask, buf, newIdxMask, len);
      if (offsetCorrect != null) {
        System.arraycopy(offsetCorrect, origIdxMask, offsetCorrect, newIdxMask, len + 1);
      }
      return correctOffset(
          newIdx,
          restoreShiftDest,
          externalOffsetAdjust,
          shift,
          insert,
          newHead,
          eoAdjustIdx,
          deletePrefix);
    } else {
      remainder = len;
      headDiffIncrement = 1;
    }
    final int requiredSpace = remainder;
    if (tmpBuf == null || requiredSpace > tmpBuf.length) {
      // incorporate `tmpOffset` in resize to avoid thrashing; but the old tmpBuf has already been
      // "retained", so we set new tmpOffset=0 and avoid copying old contents for this run.
      tmpBuf = new char[requiredSpace];
    }
    final int[] tmpOffsetBufLocal;
    if (offsetCorrect == null) {
      tmpOffsetBufLocal = null;
    } else {
      if (tmpOffsetBuf == null || remainder >= tmpOffsetBuf.length) {
        // +1 to include head diff; fine if this is oversized when headDiffIncrement==0
        tmpOffsetBuf = new int[remainder + 1];
      }
      tmpOffsetBufLocal = tmpOffsetBuf;
    }
    _getChars(
        origIdxMask, origIdxMask + remainder, tmpBuf, tmpOffsetBufLocal, 0, headDiffIncrement);
    _copy(
        newIdxMask,
        (newIdxMask + remainder) & mask,
        tmpBuf,
        tmpOffsetBufLocal,
        0,
        remainder,
        headDiffIncrement);
    return correctOffset(
        newIdx,
        restoreShiftDest,
        externalOffsetAdjust,
        shift,
        insert,
        newHead,
        eoAdjustIdx,
        deletePrefix);
  }

  private char[] tmpBuf;
  private int[] tmpOffsetBuf;

  private void ensureCapacity(int newLen) {
    if (newLen < capacity) {
      // capacity already sufficient
      return;
    }
    final int newCapacity = Integer.highestOneBit(newLen) << 1;
    final char[] newBuf = new char[newCapacity];
    final int[] newOffsetCorrect;
    if (offsetCorrect == null) {
      newOffsetCorrect = null;
    } else {
      newOffsetCorrect = new int[newCapacity];
    }
    final int newMask = newCapacity - 1;
    final int tailOldMask = tail & mask;
    final int headOldMask = head & mask;
    final int tailNewMask = tail & newMask;
    final int headNewMask = head & newMask;
    if (tailOldMask <= headOldMask) {
      // simple case; new and old both fit without wrapping
      final int len = head - tail;
      System.arraycopy(buf, tailOldMask, newBuf, tailNewMask, len);
      if (newOffsetCorrect != null) {
        // `len+1` to include headDiff
        System.arraycopy(offsetCorrect, tailOldMask, newOffsetCorrect, tailNewMask, len + 1);
      }
    } else {
      final int tailSectionLength = capacity - tailOldMask;
      if (tailNewMask <= headNewMask) {
        // new fits without wrappping, but old doesn't
        final int newHeadSectionOffset = tailNewMask + tailSectionLength;
        System.arraycopy(buf, tailOldMask, newBuf, tailNewMask, tailSectionLength);
        System.arraycopy(buf, 0, newBuf, newHeadSectionOffset, headOldMask);
        if (newOffsetCorrect != null) {
          System.arraycopy(
              offsetCorrect, tailOldMask, newOffsetCorrect, tailNewMask, tailSectionLength);
          // `len+1` to include headDiff
          System.arraycopy(
              offsetCorrect, 0, newOffsetCorrect, newHeadSectionOffset, headOldMask + 1);
        }
      } else {
        // new and old both wrap (but at the same boundary)
        System.arraycopy(buf, tailOldMask, newBuf, tailNewMask, tailSectionLength);
        System.arraycopy(buf, 0, newBuf, 0, headOldMask);
        if (newOffsetCorrect != null) {
          System.arraycopy(
              offsetCorrect, tailOldMask, newOffsetCorrect, tailNewMask, tailSectionLength);
          // `len+1` to include headDiff
          System.arraycopy(offsetCorrect, 0, newOffsetCorrect, 0, headOldMask + 1);
        }
      }
    }
    capacity = newCapacity;
    mask = newMask;
    buf = newBuf;
    offsetCorrect = newOffsetCorrect;
  }

  @Override
  public boolean hasMetaData() {
    // can the return be externally cached? If not, we can probably do better than this.
    // but I see no actual callers of this method in ICU code, so it probably doesn't
    // matter either way?
    return true;
  }
}
