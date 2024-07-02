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

import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.Transliterator.Position;
import com.ibm.icu.text.UTF16;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.MalformedInputException;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;
import org.apache.lucene.analysis.icu.CircularReplaceable.OffsetCorrectionRegistrar;

/**
 * A {@link CharFilter} that transforms text with ICU.
 *
 * <p>This is similar to {@link ICUTransformFilter}, but is capable of operating on pre-tokenized
 * input, which can be particularly useful in cases where tokenization may be affected by
 * transliteration. This class invokes the {@link Transliterator} API in a way that supports truly
 * streaming transliteration.
 *
 * <p>ICU provides text-transformation functionality via its Transliteration API. Although script
 * conversion is its most common use, a Transliterator can actually perform a more general class of
 * tasks. In fact, Transliterator defines a very general API which specifies only that a segment of
 * the input text is replaced by new text. The particulars of this conversion are determined
 * entirely by subclasses of Transliterator.
 *
 * <p>Some useful transformations for search are built-in:
 *
 * <ul>
 *   <li>Conversion from Traditional to Simplified Chinese characters
 *   <li>Conversion from Hiragana to Katakana
 *   <li>Conversion from Fullwidth to Halfwidth forms.
 *   <li>Script conversions, for example Serbian Cyrillic to Latin
 * </ul>
 *
 * <p>Example usage:
 *
 * <blockquote>
 *
 * stream = ICUTransform2CharFilterFactory.wrap(reader,
 * Transliterator.getInstance("Traditional-Simplified"));
 *
 * </blockquote>
 *
 * <br>
 * For more details, see the <a href="http://userguide.icu-project.org/transforms/general">ICU User
 * Guide</a>.
 *
 * @see ICUTransform2CharFilterFactory#wrap(Reader, Transliterator)
 */
public final class ICUTransform2CharFilter extends BaseCharFilter {

  private final int maxKeepContext;
  private final Transliterator[] leaves;
  private final IntUnaryOperator[] bypassFilterFunctions;
  private final int[] mcls;

  // [ a >= b >= c >= d ]
  private final int[] committedTo;
  private final int[] limitTo;
  private final int lastIdx;
  private final CircularReplaceable buf;

  // usually `0`; `-1` (as in, `buf.length()-1`) when last char is a lead surrogate
  private int bufLimitAdjust;

  private final Position position = new Position();
  private final OffsetCorrectionRegistrar registrar;
  private boolean inputFinished = false;

  /**
   * Creates a new {@link ICUTransform2CharFilter}
   *
   * @param in - input
   * @param leaves - atomic/leaf transliterators that are joined together in a pipeline to achieve
   *     high-level transliteration
   * @param bypassFilterFunctions - bypass functions that are used to skip over leaf
   *     transliterators. this is the flat/streaming equivalent of hierarchical UnicodeFilters
   * @param maxKeepContext - upon flushing the contents of the active transliteration buffer, keep
   *     this amount of context in the buffer, as context for subsequent transliteration windows.
   * @param mcls - maximumContextLength (antecontext) for each leaf transliterator. This is similar
   *     to {@link Transliterator#getMaximumContextLength()}, but is precomputed and notably
   *     accounts for antecontext quantifiers (which {@link
   *     Transliterator#getMaximumContextLength()} does not).
   * @param registrar - callback that receives notifications of incremental offset diffs upon
   *     flushing completed portions of the transliteration buffer.
   */
  ICUTransform2CharFilter(
      Reader in,
      Transliterator[] leaves,
      IntUnaryOperator[] bypassFilterFunctions,
      int maxKeepContext,
      int[] mcls,
      OffsetCorrectionRegistrar registrar) {
    super(in);
    this.leaves = leaves;
    this.bypassFilterFunctions = bypassFilterFunctions;
    this.committedTo = new int[bypassFilterFunctions.length];
    this.limitTo = new int[bypassFilterFunctions.length];
    this.lastIdx = bypassFilterFunctions.length - 1;
    this.maxKeepContext = maxKeepContext;
    this.mcls = mcls;
    this.registrar =
        registrar != null
            ? registrar
            : new OffsetCorrectionRegistrar(
                (offset, diff) -> {
                  addOffCorrectMap(offset, diff);
                  return 0; // return value is irrelevant
                });
    buf = new CircularReplaceable(registrar != ICUTransform2CharFilterFactory.DEV_NULL_REGISTRAR);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    for (; ; ) {
      final int ret =
          buf.flush(cbuf, off, len, committedTo[lastIdx], maxKeepContext + 1, registrar);
      if (ret != 0) {
        // if anything in buffer, flush and return
        return ret;
      } else if (inputFinished) {
        buf.flushHeadDiff(registrar);
        return -1;
      }
      if (buf.readFrom(input, len) < 0) {
        bufLimitAdjust = 0;
        finishTransliterate();
      } else {
        bufLimitAdjust = UTF16.isLeadSurrogate(buf.charAt(buf.length() - 1)) ? -1 : 0;
        incrementalTransliterate(lastIdx); // lastIdx forces `nextIdx` to advance limit
      }
    }
  }

  /**
   * {@link #BATCH_BUFFER_SIZE} sets a threshold buffer size to prevent {@link
   * Transliterator#filteredTransliterate(Replaceable, Position, boolean)} from being called for
   * every new character; we can do this because offset corrections are tracked directly by
   * CircularReplaceable, so we don't need monitor externally for offset changes. Adding this buffer
   * could easily mask underlying issues, so it's important to be able to modify/disable it for
   * tests, running tests with 0 tolerance/overhead.
   */
  // non-final, package-access for tests
  static int BATCH_BUFFER_SIZE;

  static final int DEFAULT_BATCH_BUFFER_SIZE = 16;

  static {
    // TODO: remove this system property, it isn't needed and will cause security issues
    String batchProp = System.getProperty("icu.batchBufferSize");
    BATCH_BUFFER_SIZE = batchProp == null ? DEFAULT_BATCH_BUFFER_SIZE : Integer.parseInt(batchProp);
  }

  private void incrementalTransliterate(final int initIdx) throws MalformedInputException {
    int i = initIdx;
    leafLoop:
    while ((i = nextIdx(i + 1)) < leaves.length) {
      final Transliterator leaf = leaves[i];
      final int preStart = committedTo[i];
      position.contextStart = Math.max(0, getContextStart(i, preStart));
      position.start = preStart;
      int maxLimit;
      if (i == 0) {
        maxLimit = buf.length() + bufLimitAdjust;
      } else if (!inputFinished || committedTo[i - 1] < buf.length()) {
        maxLimit = getContextStart(i, advanceCeiling(i));
        if (maxLimit <= limitTo[i]) {
          i = lastIdx;
          continue;
        }
        if (UTF16.isLeadSurrogate(buf.charAt(maxLimit - 1))) {
          if (--maxLimit <= limitTo[i]) {
            i = lastIdx;
            continue;
          }
        }
      } else {
        maxLimit = buf.length();
      }
      int preLimit = limitTo[i];
      if (BATCH_BUFFER_SIZE > 0) {
        final IntUnaryOperator f = bypassFilterFunctions[i];
        while (preLimit++ < maxLimit - 1) {
          if (!UTF16.isLeadSurrogate(buf.charAt(preLimit))) {
            // the most common case
            final int bypassIdx;
            if (f != null && i != (bypassIdx = checkBypassIdx(f, preLimit, i))) {
              // next codepoint not accepted by filters for this leaf
              i = bypassIdx - 1;
              continue leafLoop;
            }
          } else if (++preLimit < maxLimit) {
            if (UTF16.isLeadSurrogate(buf.charAt(preLimit))) {
              // detect and throw as soon as we detect this situation, because it could result
              // in arbitrary-size buffering
              throw new MalformedInputException(preLimit + 1);
            }
            final int bypassIdx;
            if (f != null && i != (bypassIdx = checkBypassIdx(f, preLimit - 1, i))) {
              // next codepoint not accepted by filters for this leaf
              i = bypassIdx - 1;
              continue leafLoop;
            }
          }
        }
        assert preLimit == maxLimit;
        preLimit--;
        if (!inputFinished && maxLimit - preStart < BATCH_BUFFER_SIZE) {
          // need more input; start from the top
          limitTo[i] = maxLimit;
          i = lastIdx;
          continue;
        }
      }
      final IntUnaryOperator f = bypassFilterFunctions[i];
      for (; ; ) {
        position.limit = ++preLimit;
        position.contextLimit = preLimit;
        // String prePos = REPORT ? position.toString() : null;
        // String pre = REPORT ? toString(buf, preStart, preLimit) : null;
        final boolean incremental = !(inputFinished && preLimit == buf.length());
        leaf.filteredTransliterate(buf, position, incremental);
        // if (REPORT) System.err.println("XXX"+i+": \""+pre+"\" => \""+toString(buf, preStart,
        // position.limit)+"\" (incremental="+incremental+", "+prePos+"=>"+position+")");
        assert position.start <= position.limit;
        if (position.start == preStart) {
          // no advance at all
          if (position.limit != preLimit) {
            // cursor did not advance, but there _was_ a change (characters were deleted)
            // correct previous offset pointers
            final int diff = position.limit - preLimit;
            for (int j = i - 1; j >= 0; j--) {
              committedTo[j] += diff;
              limitTo[j] += diff;
            }
            for (int j = i + 1; j <= lastIdx && limitTo[j] >= preLimit; j++) {
              limitTo[j] += diff;
            }
            maxLimit += diff;
            preLimit = position.limit;
          }
          if (preLimit < maxLimit) {
            if (!UTF16.isLeadSurrogate(buf.charAt(preLimit))) {
              // the most common case
              final int bypassIdx;
              if (f != null && i != (bypassIdx = checkBypassIdx(f, preLimit, i))) {
                // next codepoint not accepted by filters for this leaf
                i = bypassIdx - 1;
                break;
              }
              continue;
            } else if (++preLimit < maxLimit) {
              if (UTF16.isLeadSurrogate(buf.charAt(preLimit))) {
                // detect and throw as soon as we detect this situation, because it could result
                // in arbitrary-size buffering
                throw new MalformedInputException(preLimit + 1);
              }
              final int bypassIdx;
              if (f != null && i != (bypassIdx = checkBypassIdx(f, preLimit - 1, i))) {
                // next codepoint not accepted by filters for this leaf
                i = bypassIdx - 1;
                break;
              }
              continue;
            }
          } else if (!incremental) {
            committedTo[i] = preLimit;
            break;
          }
          // need more input; start from the top
          limitTo[i] = maxLimit;
          i = lastIdx;
          break;
        } else {
          if (position.start == 0 && preStart > 0) {
            assert BT_CLASSNAME.equals(leaf.getClass().getCanonicalName());
            // this is a bug in BreakTransliterator! For incremental runs where no boundaries are
            // found, it sets position.start=0! Since no boundaries were found though, we can
            // correct this by setting position.start = position.limit.
            if (MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG) {
              position.start = position.limit;
            }
          }
          commit(i, preLimit, -1);
          break;
        }
      }
      // NOTE: if you have to put something at the end of this loop, consider changing the `break`
      // statements to `continue` to the outer loop (named loop)
    }
  }

  private static final String BT_CLASSNAME = "com.ibm.icu.text.BreakTransliterator";

  /** Non-final, package access, for tests */
  static boolean MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG = true;

  static String toString(CircularReplaceable r, int start, int end) {
    String raw = r.toString(true);
    int adjust = r.length() - raw.length();
    return raw.substring(start - adjust, end - adjust);
  }

  // private static final boolean REPORT = false;

  private int commit(int i, int preLimit, int bypass) {
    final int diff;
    if (position.limit == preLimit) {
      diff = 0;
    } else {
      // correct previous offset pointers
      diff = position.limit - preLimit;
      for (int j = i - 1; j >= 0; j--) {
        committedTo[j] += diff;
        limitTo[j] += diff;
      }
      for (int j = i + 1; j <= lastIdx && limitTo[j] >= preLimit; j++) {
        limitTo[j] += diff;
      }
    }
    if (bypass >= 0) {
      assert position.start == position.limit;
      bypass += diff;
      committedTo[i] = bypass;
      limitTo[i] = bypass;
    } else {
      committedTo[i] = position.start;
      limitTo[i] = position.limit;
    }
    return diff;
  }

  private int advanceCeiling(int i) {
    // NOTE: with the arbitrary MAX_CONTEXT_LENGTH_FLOOR universally applied by
    // ICUTransform2CharFilterFactory (in support of quantifiers), the incorporation of
    // `getMaximumContextLength()` here will in many cases be practically insignificant;
    // but it is correct, in order to enforce a strict ordering of windows across
    // different Transliterator leaves, and if more nuanced maxContextLength support
    // is introduced, this will become definitely significant.
    final int preceding = i - 1;
    return committedTo[preceding] - mcls[preceding];
  }

  private int nextIdx(int i) {
    int bufMaxIdx = buf.length() + bufLimitAdjust - 1;
    for (; ; ) {
      if (i >= leaves.length) {
        if (limitTo[0] < bufMaxIdx) {
          i = 0; // start from beginning, with incremented limit
        } else {
          return leaves.length;
        }
      }
      final int maxIdx;
      if (i == 0) {
        maxIdx = bufMaxIdx;
      } else if (!inputFinished || committedTo[i - 1] < buf.length()) {
        maxIdx = getContextStart(i, advanceCeiling(i) - 1);
        if (maxIdx < 0) {
          while (--i > 0) {
            if (limitTo[i] < getContextStart(i, advanceCeiling(i))) {
              continue;
            }
          }
          if (limitTo[0] < bufMaxIdx) {
            i = 0;
            continue;
          } else {
            return leaves.length;
          }
        }
      } else {
        assert committedTo[i - 1] == buf.length();
        maxIdx = bufMaxIdx;
      }
      final IntUnaryOperator bypassFilterFunction = bypassFilterFunctions[i];
      final int bypassIdx;
      final int checkPosition = limitTo[i];
      if (checkPosition > bufMaxIdx) {
        if (!inputFinished) {
          return leaves.length;
        } else {
          assert buf.length() == checkPosition;
          if (committedTo[i] < checkPosition) {
            limitTo[i] = bufMaxIdx;
            return i;
          } else {
            i++;
            continue;
          }
        }
      }
      if (UTF16.isLeadSurrogate(buf.charAt(checkPosition))) {
        if (checkPosition < maxIdx) {
          // ensure that transliterate methods don't split a surrogate pair
          limitTo[i]++;
        } else {
          // we're looking at a split surrogate pair
          final int preserveIdx = i;
          while (--i > 0) {
            if (limitTo[i] < getContextStart(i, advanceCeiling(i))) {
              continue;
            }
          }
          if (limitTo[0] < bufMaxIdx) {
            i = 0;
            continue;
          } else if (!inputFinished) {
            return leaves.length;
          } else {
            if (committedTo[preserveIdx] < checkPosition) {
              limitTo[preserveIdx] = Math.min(bufMaxIdx, checkPosition + 1);
              return preserveIdx;
            } else {
              i = preserveIdx + 1;
              continue;
            }
          }
        }
      }
      if (bypassFilterFunction == null
          || i == (bypassIdx = checkBypassIdx(bypassFilterFunction, checkPosition, i))) {
        // the upstream codepoint was accepted by filters for this leaf
        return i;
      } else {
        i = bypassIdx;
        // because we're staying in this loop, and `checkBypassIdx(...)` may have modified
        // buf.length(), we need to recalculate `bufMaxIdx`
        bufMaxIdx = buf.length() + bufLimitAdjust - 1;
      }
    }
  }

  private int checkBypassIdx(IntUnaryOperator bypassFilterFunction, int checkPosition, int i) {
    final int codepoint = buf.char32At(checkPosition);
    final int bypassIdx = bypassFilterFunction.applyAsInt(codepoint);
    if (bypassIdx != i) {
      // the upstream codepoint was not accepted by filters for this leaf
      assert i < bypassIdx;
      final int codepointLength = UTF16.getCharCount(codepoint);
      int bypass = checkPosition + codepointLength;
      do {
        bypass += finishLeaf(i, bypass, codepointLength);
      } while (++i < bypassIdx);
    }
    return bypassIdx;
  }

  private int finishLeaf(int i, int bypass, int codepointLength) {
    final int preStart = committedTo[i];
    final int preLimit = bypass - codepointLength;
    if (preStart >= preLimit) {
      // no actual transliteration left to do
      // still update position markers
      committedTo[i] = bypass;
      limitTo[i] = bypass;
      return 0; // no change; diff=0.
    }
    final Transliterator t = leaves[i];
    position.contextStart = Math.max(0, getContextStart(i, preStart));
    position.start = preStart;
    position.limit = preLimit;
    position.contextLimit = bypass; // _not_ `preLimit`! see NOTE below
    // NOTE: we handle `contextLimit` a little differently here. This is the _only_
    // case where `contextLimit` is different (greater than) `limit`, and the reason
    // is that we want to "show" the bypassed context to each leaf Transliterator so
    // so that it knows it doesn't block waiting for context. This seems a little
    // suspect, considering that we're passing `incremental=false`, which one would
    // think should prevent such blocking? But it's guaranteed to be safe when used
    // in a "bypass" situation, because we know that `bypass <= buf.length()`

    // String prePos = REPORT ? position.toString() : null;
    // String pre = REPORT ? toString(buf, preStart, preLimit) : null;
    t.filteredTransliterate(buf, position, false); // equivalent to `finishTransliteration(...)`
    // if (REPORT) System.err.println("YYY"+i+": \""+pre+"\" => \""+toString(buf,
    // preStart, position.limit)+"\" ("+prePos+"=>"+position+")");
    return commit(i, preLimit, bypass);
  }

  private int getContextStart(int tIdx, int forStartPosition) {
    // intially we tried to respect maxContextLength when it was set to a non-zero value, but
    // even that didn't seem to work, so for now we're going with an arbitrary floor (as configured
    // in ICUTransform2CharFilterFactory)

    // it's unclear from ICU docs whether MCL is codepoint-based or char-based; we're going
    // with `char`-based here.
    int tMCL = mcls[tIdx];
    int candidate = forStartPosition - tMCL;
    if (candidate <= 0
        || candidate >= (buf.length() + bufLimitAdjust)
        || !UTF16.isTrailSurrogate(buf.charAt(candidate))) {
      // we're out of bounds, or can't have a lead surrogate, or don't need a lead surrogate
      return candidate;
    } else {
      // contract to surrogate boundary
      return candidate + 1;
    }
  }

  private void finishTransliterate() throws MalformedInputException {
    inputFinished = true;
    int limit;
    while (committedTo[lastIdx] < (limit = buf.length())) {
      for (int i = 0; i <= lastIdx; i++) {
        if (committedTo[i] < limit) {
          /*
           * `incrementalTransliterate(...)` is called with `i - 1` in order to cause `nextIdx(...)`
           * to start by evaluating at `i`.
           *
           * NOTE: we need to call `incrementalTransliterate(...)` here (as opposed to simply calling
           * `finishLeaf(...)`) because we still need to give the "downstream" transliterator leaves
           * a chance to decide (by evaluating filters) whether or not to pay attention to the input
           * that may be in (or upstream of) their "window" of the buffer and is being flushed as a
           * result of reaching EOF.
           */
          incrementalTransliterate(i - 1);
          break;
        }
      }
    }
  }
}
