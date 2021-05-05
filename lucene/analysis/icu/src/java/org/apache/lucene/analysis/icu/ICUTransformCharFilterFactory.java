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

import com.ibm.icu.impl.Utility;
import com.ibm.icu.text.FilteredNormalizer2;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UTF16;
import com.ibm.icu.text.UnicodeFilter;
import com.ibm.icu.text.UnicodeSet;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.CharFilterFactory;

/**
 * Factory for {@link ICUTransformCharFilter}.
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
public class ICUTransformCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "icuTransform";

  static final String MAX_ROLLBACK_BUFFER_CAPACITY_ARGNAME = "maxRollbackBufferCapacity";
  static final String FAIL_ON_ROLLBACK_BUFFER_OVERFLOW_ARGNAME = "failOnRollbackBufferOverflow";
  private final UnicodeSet filter;
  private final NormType leading;
  private final Transliterator transliterator;
  private final NormType trailing;
  private final int maxRollbackBufferCapacity;
  private final boolean failOnRollbackBufferOverflow;

  // TODO: add support for custom rules
  /** Creates a new ICUTransformFilterFactory */
  public ICUTransformCharFilterFactory(Map<String, String> args) {
    this(args, false, null);
  }

  /** package access, to allow tests to suppress unicode normalization externalization */
  ICUTransformCharFilterFactory(
      Map<String, String> args, boolean suppressUnicodeNormalizationExternalization, String rules) {
    super(args);
    String id = require(args, "id");
    String direction =
        get(args, "direction", Arrays.asList("forward", "reverse"), "forward", false);
    int dir = "forward".equals(direction) ? Transliterator.FORWARD : Transliterator.REVERSE;
    int tmpCapacityHint =
        getInt(
            args,
            MAX_ROLLBACK_BUFFER_CAPACITY_ARGNAME,
            ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY);
    this.maxRollbackBufferCapacity = tmpCapacityHint == -1 ? Integer.MAX_VALUE : tmpCapacityHint;
    this.failOnRollbackBufferOverflow =
        getBoolean(
            args,
            FAIL_ON_ROLLBACK_BUFFER_OVERFLOW_ARGNAME,
            ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
    final Transliterator stockTransliterator;
    if (rules == null) {
      // the usual case, retrieving a pre-packaged Transliterator
      stockTransliterator = Transliterator.getInstance(id, dir);
    } else {
      // build a Transliterator based on custom rules
      stockTransliterator = Transliterator.createFromRules(id, rules, dir);
    }
    if (suppressUnicodeNormalizationExternalization) {
      this.filter = null;
      this.leading = null;
      this.transliterator = stockTransliterator;
      this.trailing = null;
    } else {
      ExternalNormalization ext = externalizeUnicodeNormalization(stockTransliterator);
      this.filter = ext.filter;
      this.leading = ext.leading;
      this.transliterator = ext.t;
      this.trailing = ext.trailing;
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /**
   * for tests; check whether unicode normalization externalization optimization is actually applied
   */
  boolean externalizedUnicodeNormalization() {
    return leading != null || trailing != null;
  }

  private static final Reader wrapReader(
      NormType normType, Reader r, boolean leading, List<ICUBypassCharFilter.FilterAware> toReset) {
    if (normType == null || (leading && normType == lookupNormTypeByInput(r))) {
      // either no normType, or redundant (already normalized upstream)
      return r;
    }
    final Normalizer2 normalizer;
    switch (normType) {
      case NFC:
        normalizer = Normalizer2.getNFCInstance();
        break;
      case NFD:
        normalizer = Normalizer2.getNFDInstance();
        break;
      case NFKC:
        normalizer = Normalizer2.getNFKCInstance();
        break;
      case NFKD:
        normalizer = Normalizer2.getNFKDInstance();
        break;
      default:
        throw new UnsupportedOperationException("normType \"" + normType + "\" not supported");
    }
    ICUNormalizer2CharFilter ret = new ICUNormalizer2CharFilter(r, normalizer);
    toReset.add(ret);
    return ret;
  }

  /** Default ctor for compatibility with SPI */
  public ICUTransformCharFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public Reader create(Reader input) {
    if (!externalizedUnicodeNormalization()) {
      // simple case
      return new ICUTransformCharFilter(
          input, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
    }
    ICUBypassCharFilter.Pre pre = null;
    if (filter != null) {
      pre = new ICUBypassCharFilter.Pre(input, filter);
      input = pre;
    }
    List<ICUBypassCharFilter.FilterAware> toReset = new ArrayList<>(3);
    input = wrapReader(leading, input, true, toReset);
    ICUTransformCharFilter coreTransform =
        new ICUTransformCharFilter(
            input, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
    toReset.add(coreTransform);
    input = coreTransform;
    input = wrapReader(trailing, input, false, toReset);
    return filter == null ? input : new ICUBypassCharFilter.Post(input, pre, toReset);
  }

  private static final boolean ESCAPE_UNPRINTABLE = true;

  /**
   * Attempts to detect and externalize any leading or trailing top-level Unicode normalization. In
   * the event that such normalization is detected, a new "core" Transliterator (with any detected
   * pre-/post-normalization removed) is created and returned.
   *
   * <p>The creation of any new "core" Transliterator (and much of the actual code in this method)
   * is based on the {@link com.ibm.icu.text.CompoundTransliterator#toRules(boolean)} method (with
   * the boolean arg replaced by {@link #ESCAPE_UNPRINTABLE} -- always <code>true</code> in this
   * context).
   *
   * @param t the Transliterator to base modified rules on.
   * @return simple ExternalNormalization struct containing a non-null Transliterator, if possible
   *     with any leading and trailing Unicode normalization externalized. The effect of applying
   *     the resulting leading Unicode norm, Transliterator, and trailing Unicode norm, should be
   *     equivalent to the effect of applying the input Transliterator t.
   */
  private static ExternalNormalization externalizeUnicodeNormalization(Transliterator t) {
    final Transliterator[] trans = t.getElements();
    final int start;
    final int limit;
    final NormType leading;
    final NormType trailing;
    if (trans.length == 1) {
      return new ExternalNormalization(null, null, t, null);
    } else {
      final int lastIndex;
      if ((leading = unicodeNormalizationType(trans[0].getID())) != null) {
        start = 1;
        limit =
            (trailing = unicodeNormalizationType(trans[lastIndex = trans.length - 1].getID()))
                    != null
                ? lastIndex
                : trans.length;
      } else if ((trailing = unicodeNormalizationType(trans[lastIndex = trans.length - 1].getID()))
          != null) {
        start = 0;
        limit = lastIndex;
      } else {
        return new ExternalNormalization(null, null, t, null);
      }
    }
    // We do NOT call toRules() on our component transliterators, in
    // general. If we have several rule-based transliterators, this
    // yields a concatenation of the rules -- not what we want. We do
    // handle compound RBT transliterators specially -- those for which
    // compoundRBTIndex >= 0. For the transliterator at compoundRBTIndex,
    // we do call toRules() recursively.
    StringBuilder rulesSource = new StringBuilder();
    if (t.getFilter() != null) {
      // We might be a compound RBT and if we have a global
      // filter, then emit it at the top.
      rulesSource.append("::").append(t.getFilter().toPattern(ESCAPE_UNPRINTABLE)).append(ID_DELIM);
    }
    final int globalFilterEnd = rulesSource.length();
    boolean hasAnonymousRBTs = false;
    for (int i = start; i < limit; ++i) {
      String rule;

      // Anonymous RuleBasedTransliterators (inline rules and
      // ::BEGIN/::END blocks) are given IDs that begin with
      // "%Pass": use toRules() to write all the rules to the output
      // (and insert "::Null;" if we have two in a row)
      if (trans[i].getID().startsWith("%Pass")) {
        hasAnonymousRBTs = true;
        rule = trans[i].toRules(ESCAPE_UNPRINTABLE);
        if (i > start && trans[i - 1].getID().startsWith("%Pass")) rule = "::Null;" + rule;

        // we also use toRules() on CompoundTransliterators (which we
        // check for by looking for a semicolon in the ID)-- this gets
        // the list of their child transliterators output in the right
        // format
      } else if (trans[i].getID().indexOf(';') >= 0) {
        rule = trans[i].toRules(ESCAPE_UNPRINTABLE);

        // for everything else, use baseToRules()
      } else {
        rule = baseToRules(ESCAPE_UNPRINTABLE, trans[i]);
      }
      _smartAppend(rulesSource, '\n');
      rulesSource.append(rule);
      _smartAppend(rulesSource, ID_DELIM);
    }
    // Analogous to the contract for {@link com.ibm.icu.text.Transliterator#toRules(boolean)}, the
    // modified rules String should be sufficient to recreate a Transliterator based on the
    // specified input Transliterator, via {@link
    // com.ibm.icu.text.Transliterator#createFromRules(String, String, int)}.
    final String modifiedRules =
        hasAnonymousRBTs ? rulesSource.toString() : rulesSource.substring(globalFilterEnd);
    String baseId = t.getID();
    String modId = baseId.concat(baseId.lastIndexOf('/') < 0 ? "/X_NO_NORM_IO" : "_X_NO_NORM_IO");
    Transliterator replacement =
        Transliterator.createFromRules(modId, modifiedRules, Transliterator.FORWARD);
    final UnicodeFilter f = t.getFilter();
    final UnicodeSet filterAsSet;
    if (f == null) {
      filterAsSet = null;
    } else {
      filterAsSet = new UnicodeSet();
      f.addMatchSetTo(filterAsSet);
      filterAsSet.freeze();
    }
    return new ExternalNormalization(filterAsSet, leading, replacement, trailing);
  }

  static class ExternalNormalization {
    private final UnicodeSet filter;
    private final NormType leading;
    private final Transliterator t;
    private final NormType trailing;

    private ExternalNormalization(
        UnicodeSet filter, NormType leading, Transliterator t, NormType trailing) {
      this.filter = filter;
      this.leading = leading;
      this.t = t;
      this.trailing = trailing;
    }
  }

  private static final char ID_DELIM = ';';

  /**
   * These are the leading/trailing NormTypes that we expect to see manipulating i/o for
   * Transliterators. Other norm types (e.g., FCC, FCD, NFKC_CF) are _not_ expected in this context,
   * so they are not included here (i.e., in the unlikely event that they are encountered, they will
   * not be optimized)
   */
  enum NormType {
    NFD(Normalizer2.getNFDInstance()),
    NFKD(Normalizer2.getNFKDInstance()),
    NFKC(Normalizer2.getNFKCInstance()),
    NFC(Normalizer2.getNFCInstance());

    // these are static instances, so we cache them here (mainly for purpose of comparison)
    final Normalizer2 instance;

    NormType(Normalizer2 instance) {
      this.instance = instance;
    }
  }

  private static final NormType[] NORM_TYPE_VALUES;
  private static final String[] NORM_ID_PREFIXES;
  private static final Normalizer2[] NORM_INSTANCES;

  static {
    NORM_TYPE_VALUES = NormType.values();
    NORM_ID_PREFIXES = new String[NORM_TYPE_VALUES.length];
    NORM_INSTANCES = new Normalizer2[NORM_TYPE_VALUES.length];

    int i = 0;
    for (NormType n : NORM_TYPE_VALUES) {
      NORM_INSTANCES[i] = n.instance;
      NORM_ID_PREFIXES[i++] = n.name();
    }
  }

  static NormType lookupNormTypeByInput(Reader in) {
    final Normalizer2 instance;
    if (in instanceof ICUNormalizer2CharFilter) {
      instance = ((ICUNormalizer2CharFilter) in).normalizer;
      if (instance instanceof FilteredNormalizer2) {
        return null;
      }
    } else {
      return null;
    }
    return lookupNormTypeByInstance(instance);
  }

  static NormType lookupNormTypeByInstance(Normalizer2 instance) {
    int i = NORM_INSTANCES.length - 1;
    do {
      if (NORM_INSTANCES[i] == instance) {
        return NORM_TYPE_VALUES[i];
      }
    } while (i-- > 0);
    assert false : "unexpected norm instance";
    return null;
  }

  /**
   * Return true if the specified String represents the id of a NormalizationTransliterator,
   * otherwise false.
   */
  static NormType unicodeNormalizationType(String id) {
    if (id.indexOf(';') >= 0) {
      // it's compound
      return null;
    }
    if (id.startsWith("[")) {
      // remove filter serialization prefix
      id = id.substring(id.lastIndexOf(']')).stripLeading();
    }
    if (id.startsWith("Any-")) {
      id = id.substring("Any-".length());
    }
    id = id.toUpperCase(Locale.ENGLISH);
    int i = NORM_ID_PREFIXES.length;
    do {
      if (id.startsWith(NORM_ID_PREFIXES[--i])) {
        return NORM_TYPE_VALUES[i];
      }
    } while (i > 0);
    return null;
  }

  /**
   * Append c to buf, unless buf is empty or buf already ends in c. (convenience method copied from
   * {@link com.ibm.icu.text.CompoundTransliterator})
   */
  private static void _smartAppend(StringBuilder buf, char c) {
    if (buf.length() != 0 && buf.charAt(buf.length() - 1) != c) {
      buf.append(c);
    }
  }

  /**
   * This method is essentially copied from {@link
   * com.ibm.icu.text.Transliterator#baseToRules(boolean)}
   *
   * @param escapeUnprintable escape unprintable chars
   * @param t the Transliterator to dump rules for
   * @return String representing rules for the specified Transliterator
   */
  private static String baseToRules(boolean escapeUnprintable, Transliterator t) {
    // The base class implementation of toRules munges the ID into
    // the correct format. That is: foo => ::foo
    // KEEP in sync with rbt_pars
    if (escapeUnprintable) {
      StringBuffer rulesSource = new StringBuffer();
      String id = t.getID();
      for (int i = 0; i < id.length(); ) {
        int c = UTF16.charAt(id, i);
        if (!Utility.escapeUnprintable(rulesSource, c)) {
          UTF16.append(rulesSource, c);
        }
        i += UTF16.getCharCount(c);
      }
      rulesSource.insert(0, "::");
      rulesSource.append(ID_DELIM);
      return rulesSource.toString();
    }
    return "::" + t.getID() + ID_DELIM;
  }
}
