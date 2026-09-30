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

import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Factory for {@link ICUTransform2CharFilter}.
 *
 * <p>Supports the following attributes:
 *
 * <ul>
 *   <li>id (mandatory): A Transliterator ID, one from {@link Transliterator#getAvailableIDs()}
 *   <li>direction (optional): Either 'forward' or 'reverse'. Default is forward.
 * </ul>
 *
 * @see Transliterator
 * @since 8.3.0
 * @lucene.spi {@value #NAME}
 */
public class ICUTransform2CharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "icuTransform2";

  /**
   * maxContextLength does not appear to be set appropriately for some Transliterators (perhaps
   * especially `AnyTransliterator`s?). Until we can get a proper fix in place, simply set a static
   * floor for maxContextLength.
   *
   * <p>NOTE: this was "initially" set to `2` during development, but had to be bumped to `4` see
   * TestICUTransform2CharFilter.testBespoke10 for the example that motivated this increase
   * (cy-cy_FONIPA, input '\u20f8\u20df\u20fd\u20f6\u20fe\u20e3 otfr \ufb9a vhguj')
   *
   * <p>(cy-cy_FONIPA again; input '\uaa63\uaa6d jbfbu'; bumped MCLF from 6 to 7)
   *
   * <p>cy-cy_FONIPA is a frequent problem because it makes use of (commonly-matched) quantifiers,
   * and quantifiers are _not_ accounted for in {@link Transliterator#getMaximumContextLength()}.
   * Ideally we would be able to apply an arbitrary maxContextLength floor _only_ to leaf
   * transliterators that employ quantifiers. At the moment, it appears the only way to to do this
   * would be to re-parse rules and detect quantifiers ourselves (the ICU API apparently doesn't
   * offer a window into whether a Transliterator employs quantifiers).
   *
   * <p>For now, we'll set an arbitrary floor applicable across all Transliterators.
   *
   * <p>TODO: don't apply this arbitrary floor to Transliterators that are known to _not_ use
   * quantifiers (e.g., NormalizationTransliterator, ...?)
   *
   * <p>Applying this arbitrary floor effectively masks some potential issues. E.g., if selective
   * quantifier-Transliterator detection is implemented, it may (?) be crucial to insure that
   * `ICUTransform2CharFilter#advanceCeiling(int)` should block advance based on the progression of
   * the previous instance _and its anteContext_. This is done prospectively, but is practically
   * obviated by the arbitrary across-the-board MAX_CONTEXT_LENGTH_FLOOR applied here. (See
   * `TestICUTransform2CharFilter#testBespoke12()` for a possible example illustrating this case?)
   */
  private static final int MAX_CONTEXT_LENGTH_FLOOR = 7;

  private final boolean nullTransform; // special case, for completeness
  private final Transliterator[] leaves;
  private final IntUnaryOperator[] bypassFilterFunctions;
  private final int maxKeepContext;
  private final int[] mcls;

  // TODO: add support for custom rules
  /** Creates a new ICUTransform2FilterFactory */
  public ICUTransform2CharFilterFactory(Map<String, String> args) {
    this(args, null);
  }

  /** package access, to allow tests to pass in a custom Transliterator */
  ICUTransform2CharFilterFactory(Transliterator t) {
    this(new HashMap<>(0), t);
  }

  /**
   * An {@link org.apache.lucene.analysis.icu.CircularReplaceable.OffsetCorrectionRegistrar} that
   * simply ignores notifications of offset corrections. For use in cases where offset correction is
   * not required.
   */
  public static final CircularReplaceable.OffsetCorrectionRegistrar DEV_NULL_REGISTRAR =
      new CircularReplaceable.OffsetCorrectionRegistrar((offset, diff) -> 0);

  /**
   * This class is useful for streaming transliteration of documents outside of the context of
   * Lucene analysis. For that reason we provide a trivial main method to stream transliteration
   * from stdin to stdout.
   */
  @SuppressForbidden(reason = "command-line util writes to stdout")
  public static void main(String[] args) throws IOException {
    Reader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    Writer w = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    Transliterator t = Transliterator.getInstance(args[0]);
    CharFilter f = ICUTransform2CharFilterFactory.wrap(r, t, DEV_NULL_REGISTRAR);
    final char[] buf = new char[1024];
    int read;
    while ((read = f.read(buf)) != -1) {
      w.write(buf, 0, read);
    }
    w.flush();
  }

  private ICUTransform2CharFilterFactory(Map<String, String> args, Transliterator t) {
    super(args);
    if (t == null) {
      t = parseTransliteratorFromArgs(args);
    }
    // `decomposed` contains only leaf-level Transliterators
    List<TransliteratorEntry> decomposed = new ArrayList<>();
    List<List<FilterBypassEntry>> bypassFilters = new ArrayList<>();
    final int[] maxKeepContext = new int[] {t.getMaximumContextLength(), 0, 0};
    UnicodeSet topLevelFilter =
        decomposeTransliterator(t, decomposed, bypassFilters, maxKeepContext, 1);
    if (topLevelFilter != null) {
      final FilterBypassEntry topLevelEntry =
          new FilterBypassEntry(topLevelFilter, decomposed.size());
      List<FilterBypassEntry> first = bypassFilters.get(0);
      if (first == null) {
        bypassFilters.set(0, Collections.singletonList(topLevelEntry));
      } else {
        first.add(topLevelEntry);
      }
    }
    final int size = decomposed.size();
    leaves = new Transliterator[size];
    mcls = new int[size];
    int i = 0;
    for (TransliteratorEntry e : decomposed) {
      leaves[i] = e.t;
      mcls[i++] = e.maxContextLength;
    }
    bypassFilterFunctions = processBypassFilters(bypassFilters);
    this.maxKeepContext = maxKeepContext[2];
    nullTransform = size == 0;
  }

  private Transliterator parseTransliteratorFromArgs(Map<String, String> args) {
    String id = require(args, "id");
    String direction =
        get(args, "direction", Arrays.asList("forward", "reverse"), "forward", false);
    int dir = "forward".equals(direction) ? Transliterator.FORWARD : Transliterator.REVERSE;
    final Transliterator t = Transliterator.getInstance(id, dir);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
    return t;
  }

  /** Wraps reader with an ICU transform */
  public static CharFilter wrap(Reader input, Transliterator t) {
    return wrap(input, t, null);
  }

  static CharFilter wrap(
      Reader input, Transliterator t, CircularReplaceable.OffsetCorrectionRegistrar registrar) {
    return (CharFilter) new ICUTransform2CharFilterFactory(t).create(input, registrar);
  }

  private static IntUnaryOperator[] processBypassFilters(
      List<List<FilterBypassEntry>> bypassFilters) {
    final int size = bypassFilters.size();
    IntUnaryOperator[] ret = new IntUnaryOperator[size];
    int i = 0;
    for (List<FilterBypassEntry> bypassStartFilters : bypassFilters) {
      final int acceptIdx = i++;
      final int filterCount;
      if (bypassStartFilters == null) {
        ret[acceptIdx] = null;
      } else if ((filterCount = bypassStartFilters.size()) == 1) {
        final FilterBypassEntry e = bypassStartFilters.get(0);
        final UnicodeSet filter = e.filter;
        final int bypassIdx = e.bypassIdx;
        ret[acceptIdx] = (codepoint) -> filter.contains(codepoint) ? acceptIdx : bypassIdx;
      } else {
        assert !bypassStartFilters.isEmpty();
        final FilterBypassEntry[] filters =
            bypassStartFilters.toArray(new FilterBypassEntry[filterCount]);
        ret[acceptIdx] =
            (codepoint) -> {
              int j = filterCount;
              do {
                FilterBypassEntry e = filters[--j];
                if (!e.filter.contains(codepoint)) {
                  return e.bypassIdx;
                }
              } while (j > 0);
              return acceptIdx; // passed all filters
            };
      }
    }
    return ret;
  }

  private static UnicodeSet decomposeAnyTransliterator(
      Transliterator parent,
      List<TransliteratorEntry> decomposed,
      List<List<FilterBypassEntry>> bypassFilters,
      int[] maxKeepContext,
      int depth) {
    // we'll initially treat this as an atomic leaf
    final int localMaxCl = Math.max(parent.getMaximumContextLength(), MAX_CONTEXT_LENGTH_FLOOR);
    updateMaxKeepContext(localMaxCl, maxKeepContext);
    decomposed.add(new TransliteratorEntry(parent, localMaxCl));
    bypassFilters.add(null);
    return null; // filter remains integrated in leaves
  }

  private static void updateMaxKeepContext(int localMaxCl, int[] maxKeepContext) {
    final int handOverHandMaxCl = localMaxCl + maxKeepContext[1];
    if (handOverHandMaxCl > maxKeepContext[2]) {
      maxKeepContext[2] = handOverHandMaxCl;
    }
    maxKeepContext[1] = localMaxCl;
  }

  private static UnicodeSet decomposeTransliterator(
      Transliterator parent,
      List<TransliteratorEntry> decomposed,
      List<List<FilterBypassEntry>> bypassFilters,
      int[] maxKeepContext,
      int depth) {
    final Transliterator[] elements = parent.getElements();
    if (elements.length == 1 && elements[0] == parent) {
      final int localMaxCl;
      switch (parent.getClass().getCanonicalName()) {
        case "com.ibm.icu.text.AnyTransliterator":
          // AnyTransliterators are effectively composite/multiplexed, so we have to jump
          // through some extra hoops to decompose them
          return decomposeAnyTransliterator(
              parent, decomposed, bypassFilters, maxKeepContext, depth);
        case "com.ibm.icu.text.NullTransliterator":
          // these can be ignored
          return null;
        case "com.ibm.icu.text.NormalizationTransliterator":
          // known to _not_ have quantifiers
          // maxContextLength = parent.getMaximumContextLength();
          localMaxCl = parent.getMaximumContextLength();
          break;
        default:
          // including RuleBasedTransliterator, some of which are known to have quantifiers
          localMaxCl = Math.max(parent.getMaximumContextLength(), MAX_CONTEXT_LENGTH_FLOOR);
          break;
      }
      // leaf Transliterator, base case
      updateMaxKeepContext(localMaxCl, maxKeepContext);
      decomposed.add(new TransliteratorEntry(parent, localMaxCl));
      bypassFilters.add(null);
      return null; // filter remains integrated in leaves
    }
    for (Transliterator t : elements) {
      final int localMaxContextLen = t.getMaximumContextLength();
      if (localMaxContextLen > maxKeepContext[0]) {
        maxKeepContext[0] = localMaxContextLen;
      }
      final int filterAcceptIdx = bypassFilters.size();
      final UnicodeSet f =
          decomposeTransliterator(t, decomposed, bypassFilters, maxKeepContext, depth + 1);
      if (f != null) {
        final int filterRejectIdx = bypassFilters.size();
        List<FilterBypassEntry> bypassStartFilters = bypassFilters.get(filterAcceptIdx);
        if (bypassStartFilters == null) {
          bypassFilters.set(filterAcceptIdx, bypassStartFilters = new ArrayList<>(depth));
        }
        bypassStartFilters.add(new FilterBypassEntry(f, filterRejectIdx));
      }
    }
    return (UnicodeSet) parent.getFilter();
  }

  private static class TransliteratorEntry {
    private final Transliterator t;
    private final int maxContextLength;

    private TransliteratorEntry(Transliterator t, int maxContextLength) {
      this.t = t;
      this.maxContextLength = maxContextLength;
    }
  }

  private static class FilterBypassEntry {
    private final UnicodeSet filter;
    private final int bypassIdx;

    private FilterBypassEntry(UnicodeSet filter, int bypassIdx) {
      this.filter = filter;
      this.bypassIdx = bypassIdx;
    }
  }

  /** Default ctor for compatibility with SPI */
  public ICUTransform2CharFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public Reader create(Reader input) {
    return create(input, null);
  }

  /**
   * Similar to {@link #create(Reader)}, but allows to specify an external {@link
   * org.apache.lucene.analysis.icu.CircularReplaceable.OffsetCorrectionRegistrar}. This is useful
   * for tests, or in contexts other than Analysis chain.
   *
   * @param input - input Reader
   * @param registrar - callback to receive notifications of offset diffs
   * @return - a CharFilter for applying the Transliteration configured for this {@link
   *     ICUNormalizer2CharFilterFactory}.
   */
  public Reader create(Reader input, CircularReplaceable.OffsetCorrectionRegistrar registrar) {
    if (nullTransform) {
      return new NullTransformCharFilter(input);
    } else {
      return new ICUTransform2CharFilter(
          input, leaves, bypassFilterFunctions, maxKeepContext, mcls, registrar);
    }
  }

  private static class NullTransformCharFilter extends BaseCharFilter {
    public NullTransformCharFilter(Reader in) {
      super(in);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      return input.read(cbuf, off, len);
    }
  }
}
