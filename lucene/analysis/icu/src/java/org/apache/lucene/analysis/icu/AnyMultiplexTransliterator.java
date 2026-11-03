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

import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeFilter;
import java.util.MissingResourceException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is like an AnyTransliterator, but it doesn't actually _do_ anything; just blocks on "COMMON"
 * or "INHERITED" characters until we know which
 */
class AnyMultiplexTransliterator extends Transliterator {

  private static final int MAX_CONTEXT_LENGTH = 5;

  // TODO: this should ultimately be used or removed
  @SuppressWarnings("unused")
  private final int maxContextLength;

  /**
   * Return an {@link AnyMultiplexTransliterator} instance representing the `AnyTransliterator`
   * defined by the specified ID.
   */
  public static AnyMultiplexTransliterator getInstance(Transliterator t) {
    return new AnyMultiplexTransliterator(t.getID(), t.getFilter(), MAX_CONTEXT_LENGTH);
  }

  /**
   * Create a new {@link AnyMultiplexTransliterator} for the specified ID, with the specified {@link
   * UnicodeFilter} and max context length
   *
   * @param ID - the ID of the transform
   * @param filter - restricts input codepoints
   * @param maxContextLength - see {@link Transliterator#getMaximumContextLength()}
   */
  protected AnyMultiplexTransliterator(String ID, UnicodeFilter filter, int maxContextLength) {
    super(ID, filter);
    String[] stv = IDtoSTV(ID);
    final String targetScriptName = stv[1];
    this.targetScript = scriptNameToCode(targetScriptName);
    final String targetVariantName = stv[2];
    if (targetVariantName.isEmpty()) {
      this.target = targetScriptName;
    } else {
      this.target = targetScriptName + VARIANT_SEP + targetVariantName;
    }
    this.maxContextLength = maxContextLength;
  }

  @Override
  protected void handleTransliterate(Replaceable text, Position pos, boolean incremental) {}

  /** copied from com.ibm.icu.text.TransliteratorIDParser */
  public static String[] IDtoSTV(String id) {
    String source = ANY;
    String target = null;
    String variant = "";

    int sep = id.indexOf(TARGET_SEP);
    int varPos = id.indexOf(VARIANT_SEP);
    if (varPos < 0) {
      varPos = id.length();
    }
    boolean isSourcePresent = false;

    if (sep < 0) {
      // Form: T/V or T (or /V)
      target = id.substring(0, varPos);
      variant = id.substring(varPos);
    } else if (sep < varPos) {
      // Form: S-T/V or S-T (or -T/V or -T)
      if (sep > 0) {
        source = id.substring(0, sep);
        isSourcePresent = true;
      }
      target = id.substring(++sep, varPos);
      variant = id.substring(varPos);
    } else {
      // Form: (S/V-T or /V-T)
      if (varPos > 0) {
        source = id.substring(0, varPos);
        isSourcePresent = true;
      }
      variant = id.substring(varPos, sep++);
      target = id.substring(sep);
    }

    if (variant.length() > 0) {
      variant = variant.substring(1);
    }

    return new String[] {source, target, variant, isSourcePresent ? "" : null};
  }

  // BEGIN copied from com.ibm.icu.text.AnyTransliterator

  private static int scriptNameToCode(String name) {
    try {
      int[] codes = UScript.getCode(name);
      return codes != null ? codes[0] : UScript.INVALID_CODE;
    } catch (
        @SuppressWarnings("unused")
        MissingResourceException e) {
      return UScript.INVALID_CODE;
    }
  }

  static final char TARGET_SEP = '-';
  static final char VARIANT_SEP = '/';
  static final String ANY = "Any";
  static final String LATIN_PIVOT = "-Latin;Latin-";

  /** Cache mapping UScriptCode values to Transliterator*. */
  private ConcurrentHashMap<Integer, Transliterator> cache;

  /** The target or target/variant string. */
  private final String target;

  /** The target script code. Never USCRIPT_INVALID_CODE. */
  private final int targetScript;

  /** Special code for handling width characters */
  private static final String WIDTH_FIX_SPEC = "[[:dt=Nar:][:dt=Wide:]] nfkd;";
  // leading Null necessary to prevent filter from being interpreted as a global filter
  private static final String COMPOSITE_WIDTH_FIX_SPEC = "Null;".concat(WIDTH_FIX_SPEC);
  private static final Transliterator widthFix = Transliterator.getInstance(WIDTH_FIX_SPEC);

  private boolean isWide(int script) {
    return script == UScript.BOPOMOFO
        || script == UScript.HAN
        || script == UScript.HANGUL
        || script == UScript.HIRAGANA
        || script == UScript.KATAKANA;
  }

  // TODO: this should ultimately be used or removed
  @SuppressWarnings("unused")
  private Transliterator getTransliterator(int source) {
    if (source == targetScript || source == UScript.INVALID_CODE) {
      if (isWide(targetScript)) {
        return null;
      } else {
        return widthFix;
      }
    }

    Integer key = Integer.valueOf(source);
    Transliterator t = cache.get(key);
    if (t == null) {
      String sourceName = UScript.getName(source);
      String id = sourceName + TARGET_SEP + target;

      try {
        t = Transliterator.getInstance(id, FORWARD);
      } catch (RuntimeException e) {
      }
      if (t == null) {

        // Try to pivot around Latin, our most common script
        id = sourceName + LATIN_PIVOT + target;
        try {
          t = Transliterator.getInstance(id, FORWARD);
        } catch (RuntimeException e) {
        }
      }

      if (t != null) {
        if (!isWide(targetScript)) {
          t = Transliterator.getInstance(COMPOSITE_WIDTH_FIX_SPEC.concat(id), FORWARD);
        }
        Transliterator prevCachedT = cache.putIfAbsent(key, t);
        if (prevCachedT != null) {
          t = prevCachedT;
        }
      } else if (!isWide(targetScript)) {
        return widthFix;
      }
    }

    return t;
  }
  // END copied from com.ibm.icu.text.AnyTransliterator
}
