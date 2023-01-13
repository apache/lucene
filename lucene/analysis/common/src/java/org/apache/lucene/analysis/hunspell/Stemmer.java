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
package org.apache.lucene.analysis.hunspell;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;

/**
 * Stemmer uses the affix rules declared in the Dictionary to generate one or more stems for a word.
 * It conforms to the algorithm in the original hunspell algorithm, including recursive suffix
 * stripping.
 */
final class Stemmer {
  private final Dictionary dictionary;

  // it's '1' if we have no stem exceptions, otherwise every other form
  // is really an ID pointing to the exception table
  private final int formStep;

  /**
   * Constructs a new Stemmer which will use the provided Dictionary to create its stems.
   *
   * @param dictionary Dictionary that will be used to create the stems
   */
  public Stemmer(Dictionary dictionary) {
    this.dictionary = dictionary;
    formStep = dictionary.formStep();
  }

  /**
   * Find the stem(s) of the provided word.
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> stem(String word) {
    return stem(word.toCharArray(), word.length());
  }

  /**
   * Find the stem(s) of the provided word
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> stem(char[] word, int length) {
    List<CharsRef> list = new ArrayList<>();
    analyze(
        word,
        length,
        (stem, formID, morphDataId, outerPrefix, innerPrefix, outerSuffix, innerSuffix) -> {
          list.add(newStem(stem, morphDataId));
          return true;
        });
    return list;
  }

  void analyze(char[] word, int length, RootProcessor processor) {
    if (dictionary.mayNeedInputCleaning()) {
      CharsRef scratchSegment = new CharsRef(word, 0, length);
      if (dictionary.needsInputCleaning(scratchSegment)) {
        StringBuilder segment = new StringBuilder();
        dictionary.cleanInput(scratchSegment, segment);
        char[] scratchBuffer = new char[segment.length()];
        length = segment.length();
        segment.getChars(0, length, scratchBuffer, 0);
        word = scratchBuffer;
      }
    }
    if (length == 0) {
      return;
    }

    if (!doStem(word, 0, length, WordContext.SIMPLE_WORD, processor)) {
      return;
    }

    WordCase wordCase = caseOf(word, length);
    if (wordCase == WordCase.UPPER || wordCase == WordCase.TITLE) {
      CaseVariationProcessor variationProcessor =
          (variant, varLength, originalCase) ->
              doStem(variant, 0, varLength, WordContext.SIMPLE_WORD, processor);
      varyCase(word, length, wordCase, variationProcessor);
    }
  }

  interface CaseVariationProcessor {
    boolean process(char[] word, int length, WordCase originalCase);
  }

  boolean varyCase(char[] word, int length, WordCase wordCase, CaseVariationProcessor processor) {
    char[] titleBuffer = wordCase == WordCase.UPPER ? caseFoldTitle(word, length) : null;
    if (wordCase == WordCase.UPPER) {
      char[] aposCase = capitalizeAfterApostrophe(titleBuffer, length);
      if (aposCase != null && !processor.process(aposCase, length, wordCase)) {
        return false;
      }
      if (!processor.process(titleBuffer, length, wordCase)) {
        return false;
      }
      if (dictionary.checkSharpS && !varySharpS(titleBuffer, length, processor)) {
        return false;
      }
    }

    if (dictionary.isDotICaseChangeDisallowed(word)) {
      return true;
    }

    char[] lowerBuffer = caseFoldLower(titleBuffer != null ? titleBuffer : word, length);
    if (!processor.process(lowerBuffer, length, wordCase)) {
      return false;
    }
    if (wordCase == WordCase.UPPER
        && dictionary.checkSharpS
        && !varySharpS(lowerBuffer, length, processor)) {
      return false;
    }
    return true;
  }

  /** returns EXACT_CASE,TITLE_CASE, or UPPER_CASE type for the word */
  WordCase caseOf(char[] word, int length) {
    if (dictionary.ignoreCase || length == 0 || Character.isLowerCase(word[0])) {
      return WordCase.MIXED;
    }

    return WordCase.caseOf(word, length);
  }

  /** folds titlecase variant of word to titleBuffer */
  private char[] caseFoldTitle(char[] word, int length) {
    char[] titleBuffer = new char[length];
    System.arraycopy(word, 0, titleBuffer, 0, length);
    for (int i = 1; i < length; i++) {
      titleBuffer[i] = dictionary.caseFold(titleBuffer[i]);
    }
    return titleBuffer;
  }

  /** folds lowercase variant of word (title cased) to lowerBuffer */
  private char[] caseFoldLower(char[] word, int length) {
    char[] lowerBuffer = new char[length];
    System.arraycopy(word, 0, lowerBuffer, 0, length);
    lowerBuffer[0] = dictionary.caseFold(lowerBuffer[0]);
    return lowerBuffer;
  }

  // Special prefix handling for Catalan, French, Italian:
  // prefixes separated by apostrophe (SANT'ELIA -> Sant'+Elia).
  private static char[] capitalizeAfterApostrophe(char[] word, int length) {
    for (int i = 1; i < length - 1; i++) {
      if (word[i] == '\'') {
        char next = word[i + 1];
        char upper = Character.toUpperCase(next);
        if (upper != next) {
          char[] copy = ArrayUtil.copyOfSubArray(word, 0, length);
          copy[i + 1] = Character.toUpperCase(upper);
          return copy;
        }
      }
    }
    return null;
  }

  private boolean varySharpS(char[] word, int length, CaseVariationProcessor processor) {
    Stream<String> result =
        new Object() {
          int findSS(int start) {
            for (int i = start; i < length - 1; i++) {
              if (word[i] == 's' && word[i + 1] == 's') {
                return i;
              }
            }
            return -1;
          }

          Stream<String> replaceSS(int start, int depth) {
            if (depth > 5) { // cut off too large enumeration
              return Stream.of(new String(word, start, length - start));
            }

            int ss = findSS(start);
            if (ss < 0) {
              return null;
            } else {
              String prefix = new String(word, start, ss - start);
              Stream<String> tails = replaceSS(ss + 2, depth + 1);
              if (tails == null) {
                tails = Stream.of(new String(word, ss + 2, length - ss - 2));
              }
              return tails.flatMap(s -> Stream.of(prefix + "ss" + s, prefix + "ß" + s));
            }
          }
        }.replaceSS(0, 0);
    if (result == null) return true;

    String src = new String(word, 0, length);
    for (String s : result.collect(Collectors.toList())) {
      if (!s.equals(src) && !processor.process(s.toCharArray(), s.length(), null)) {
        return false;
      }
    }
    return true;
  }

  boolean doStem(
      char[] word, int offset, int length, WordContext context, RootProcessor processor) {
    IntsRef forms = dictionary.lookupWord(word, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        int entryId = forms.ints[forms.offset + i];
        // we can't add this form, it's a pseudostem requiring an affix
        if (dictionary.hasFlag(entryId, dictionary.needaffix)) {
          continue;
        }
        if ((context == WordContext.COMPOUND_BEGIN || context == WordContext.COMPOUND_MIDDLE)
            && dictionary.hasFlag(entryId, dictionary.compoundForbid)) {
          return false;
        }
        if (!isRootCompatibleWithContext(context, -1, entryId)) {
          continue;
        }
        CharsRef charsRef = new CharsRef(word, offset, length);
        if (!processor.processRoot(charsRef, entryId, morphDataId(forms, i), -1, -1, -1, -1)) {
          return false;
        }
      }
    }
    StemCandidateProcessor stemProcessor =
        new StemCandidateProcessor(context) {
          @Override
          boolean processStemCandidate(
              char[] word,
              int offset,
              int length,
              int lastAffix,
              int outerPrefix,
              int innerPrefix,
              int outerSuffix,
              int innerSuffix) {
            IntsRef forms = dictionary.lookupWord(word, offset, length);
            if (forms == null) return true;

            char flag = dictionary.affixData(lastAffix, Dictionary.AFFIX_FLAG);
            int prefixId = innerPrefix >= 0 ? innerPrefix : outerPrefix;
            for (int i = 0; i < forms.length; i += formStep) {
              int entryId = forms.ints[forms.offset + i];
              if (dictionary.hasFlag(entryId, flag)
                  || dictionary.isFlagAppendedByAffix(prefixId, flag)) {
                if (innerPrefix < 0 && outerPrefix >= 0) {
                  char prefixFlag = dictionary.affixData(outerPrefix, Dictionary.AFFIX_FLAG);
                  if (!dictionary.hasFlag(entryId, prefixFlag)
                      && !dictionary.isFlagAppendedByAffix(lastAffix, prefixFlag)) {
                    continue;
                  }
                }

                if (!isRootCompatibleWithContext(context, lastAffix, entryId)) {
                  continue;
                }

                if (!processor.processRoot(
                    new CharsRef(word, offset, length),
                    entryId,
                    morphDataId(forms, i),
                    outerPrefix,
                    innerPrefix,
                    outerSuffix,
                    innerSuffix)) {
                  return false;
                }
              }
            }
            return true;
          }
        };
    return removeAffixes(word, offset, length, true, -1, -1, -1, stemProcessor);
  }

  /**
   * Find the unique stem(s) of the provided word
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> uniqueStems(char[] word, int length) {
    List<CharsRef> stems = stem(word, length);
    if (stems.size() < 2) {
      return stems;
    }
    CharArraySet terms = new CharArraySet(8, dictionary.ignoreCase);
    List<CharsRef> deduped = new ArrayList<>();
    for (CharsRef s : stems) {
      if (!terms.contains(s)) {
        deduped.add(s);
        terms.add(s);
      }
    }
    return deduped;
  }

  interface RootProcessor {
    /**
     * @param stem the text of the found dictionary entry
     * @param formID internal id of the dictionary entry, e.g. to be used in {@link
     *     Dictionary#hasFlag(int, char)}
     * @param morphDataId the id of the custom morphological data (0 if none), to be used with
     *     {@link Dictionary#morphData}
     * @param outerPrefix the id of the outer prefix applied to the stem, or -1 if none
     * @param innerPrefix the id of the inner prefix applied to the stem, or -1 if none
     * @param outerSuffix the id of the outer suffix applied to the stem, or -1 if none
     * @param innerSuffix the id of the inner suffix applied to the stem, or -1 if none
     * @return whether the processing should be continued
     */
    boolean processRoot(
        CharsRef stem,
        int formID,
        int morphDataId,
        int outerPrefix,
        int innerPrefix,
        int outerSuffix,
        int innerSuffix);
  }

  private String stemException(int morphDataId) {
    if (morphDataId > 0) {
      String data = dictionary.morphData.get(morphDataId);
      int start = data.startsWith("st:") ? 0 : data.indexOf(" st:");
      if (start >= 0) {
        int nextSpace = data.indexOf(' ', start + 3);
        return data.substring(start + 3, nextSpace < 0 ? data.length() : nextSpace);
      }
    }
    return null;
  }

  private CharsRef newStem(CharsRef stem, int morphDataId) {
    String exception = stemException(morphDataId);

    if (dictionary.oconv != null) {
      StringBuilder scratchSegment = new StringBuilder();
      if (exception != null) {
        scratchSegment.append(exception);
      } else {
        scratchSegment.append(stem.chars, stem.offset, stem.length);
      }
      dictionary.oconv.applyMappings(scratchSegment);
      char[] cleaned = new char[scratchSegment.length()];
      scratchSegment.getChars(0, cleaned.length, cleaned, 0);
      return new CharsRef(cleaned, 0, cleaned.length);
    } else {
      if (exception != null) {
        return new CharsRef(exception);
      } else {
        return stem;
      }
    }
  }

  /**
   * Generates a list of stems for the provided word. It's called recursively when applying affixes
   * one by one, setting {@code (inner/outer)(Suffix/Prefix)} parameters to non-negative values as
   * that happens.
   *
   * @param word Word to generate the stems for
   * @param doPrefix true if we should remove prefixes
   * @return whether the processing should be continued
   */
  boolean removeAffixes(
      char[] word,
      int offset,
      int length,
      boolean doPrefix,
      int outerPrefix,
      int innerPrefix,
      int outerSuffix,
      StemCandidateProcessor processor) {
    FST.Arc<IntsRef> arc = new FST.Arc<>();
    if (doPrefix && dictionary.prefixes != null) {
      FST<IntsRef> fst = dictionary.prefixes;
      FST.BytesReader reader = fst.getBytesReader();
      fst.getFirstArc(arc);
      IntsRef output = fst.outputs.getNoOutput();
      int limit = dictionary.fullStrip ? length + 1 : length;
      for (int i = 0; i < limit; i++) {
        if (i > 0) {
          output = Dictionary.nextArc(fst, arc, reader, output, word[offset + i - 1]);
          if (output == null) {
            break;
          }
        }
        if (!arc.isFinal()) {
          continue;
        }
        IntsRef prefixes = fst.outputs.add(output, arc.nextFinalOutput());

        for (int j = 0; j < prefixes.length; j++) {
          int prefix = prefixes.ints[prefixes.offset + j];
          if (prefix == outerPrefix) {
            continue;
          }

          if (isAffixCompatible(prefix, true, outerPrefix, outerSuffix, processor.context)) {
            char[] strippedWord = stripAffix(word, offset, length, i, prefix, true);
            if (strippedWord == null) {
              continue;
            }

            boolean pureAffix = strippedWord == word;
            if (!applyAffix(
                strippedWord,
                pureAffix ? offset + i : 0,
                pureAffix ? length - i : strippedWord.length,
                prefix,
                true,
                outerPrefix,
                innerPrefix,
                outerSuffix,
                processor)) {
              return false;
            }
          }
        }
      }
    }

    if (dictionary.suffixes != null) {
      FST<IntsRef> fst = dictionary.suffixes;
      FST.BytesReader reader = fst.getBytesReader();
      fst.getFirstArc(arc);
      IntsRef output = fst.outputs.getNoOutput();
      int limit = dictionary.fullStrip ? 0 : 1;
      for (int i = length; i >= limit; i--) {
        if (i < length) {
          output = Dictionary.nextArc(fst, arc, reader, output, word[offset + i]);
          if (output == null) {
            break;
          }
        }
        if (!arc.isFinal()) {
          continue;
        }
        IntsRef suffixes = fst.outputs.add(output, arc.nextFinalOutput());

        for (int j = 0; j < suffixes.length; j++) {
          int suffix = suffixes.ints[suffixes.offset + j];
          if (suffix == outerSuffix) {
            continue;
          }

          if (isAffixCompatible(suffix, false, outerPrefix, outerSuffix, processor.context)) {
            char[] strippedWord = stripAffix(word, offset, length, length - i, suffix, false);
            if (strippedWord == null) {
              continue;
            }

            boolean pureAffix = strippedWord == word;
            if (!applyAffix(
                strippedWord,
                pureAffix ? offset : 0,
                pureAffix ? i : strippedWord.length,
                suffix,
                false,
                outerPrefix,
                innerPrefix,
                outerSuffix,
                processor)) {
              return false;
            }
          }
        }
      }
    }

    return true;
  }

  /**
   * @return null if affix conditions isn't met; a reference to the same char[] if the affix has no
   *     strip data and can thus be simply removed, or a new char[] containing the word affix
   *     removal
   */
  private char[] stripAffix(
      char[] word, int offset, int length, int affixLen, int affix, boolean isPrefix) {
    int deAffixedLen = length - affixLen;

    int stripOrd = dictionary.affixData(affix, Dictionary.AFFIX_STRIP_ORD);
    int stripStart = dictionary.stripOffsets[stripOrd];
    int stripEnd = dictionary.stripOffsets[stripOrd + 1];
    int stripLen = stripEnd - stripStart;

    if (stripLen + deAffixedLen == 0) return null;

    char[] stripData = dictionary.stripData;
    int condition = dictionary.getAffixCondition(affix);
    if (condition != 0) {
      int deAffixedOffset = isPrefix ? offset + affixLen : offset;
      if (!dictionary.patterns.get(condition).acceptsStem(word, deAffixedOffset, deAffixedLen)) {
        return null;
      }
    }

    if (stripLen == 0) return word;

    char[] strippedWord = new char[stripLen + deAffixedLen];
    System.arraycopy(
        word,
        offset + (isPrefix ? affixLen : 0),
        strippedWord,
        isPrefix ? stripLen : 0,
        deAffixedLen);
    System.arraycopy(stripData, stripStart, strippedWord, isPrefix ? 0 : deAffixedLen, stripLen);
    return strippedWord;
  }

  private boolean isAffixCompatible(
      int affix, boolean isPrefix, int outerPrefix, int outerSuffix, WordContext context) {
    int append = dictionary.affixData(affix, Dictionary.AFFIX_APPEND);

    boolean previousWasPrefix = outerSuffix < 0 && outerPrefix >= 0;
    if (context.isCompound()) {
      if (!isPrefix && dictionary.hasFlag(append, dictionary.compoundForbid)) {
        return false;
      }
      if (!context.isAffixAllowedWithoutSpecialPermit(isPrefix)
          && !dictionary.hasFlag(append, dictionary.compoundPermit)) {
        return false;
      }
      if (context == WordContext.COMPOUND_END
          && !isPrefix
          && !previousWasPrefix
          && dictionary.hasFlag(append, dictionary.onlyincompound)) {
        return false;
      }
    } else if (dictionary.hasFlag(append, dictionary.onlyincompound)) {
      return false;
    }

    if (outerPrefix == -1 && outerSuffix == -1) {
      return true;
    }

    if (dictionary.isCrossProduct(affix)) {
      // cross-check incoming continuation class (flag of previous affix) against this affix's flags
      if (previousWasPrefix) return true;
      if (outerSuffix >= 0) {
        char prevFlag = dictionary.affixData(outerSuffix, Dictionary.AFFIX_FLAG);
        return dictionary.hasFlag(append, prevFlag);
      }
    }

    return false;
  }

  /**
   * Applies the affix rule to the given word, producing a list of stems if any are found.
   * Non-negative {@code (inner/outer)(Suffix/Prefix)} parameters indicate the already applied
   * affixes.
   *
   * @param word Char array containing the word with the affix removed and the strip added
   * @param offset where the word actually starts in the array
   * @param length the length of the stripped word
   * @param affix the id of the affix in {@link Dictionary#affixData}
   * @param prefix true if we are removing a prefix (false if it's a suffix)
   * @return whether the processing should be continued
   */
  private boolean applyAffix(
      char[] word,
      int offset,
      int length,
      int affix,
      boolean prefix,
      int outerPrefix,
      int innerPrefix,
      int outerSuffix,
      StemCandidateProcessor processor) {
    int prefixId = innerPrefix >= 0 ? innerPrefix : outerPrefix;
    int previousAffix = outerSuffix >= 0 ? outerSuffix : prefixId;

    int innerSuffix = -1;
    if (prefix) {
      if (outerPrefix < 0) outerPrefix = affix;
      else innerPrefix = affix;
    } else {
      if (outerSuffix < 0) outerSuffix = affix;
      else innerSuffix = affix;
    }

    boolean skipLookup = needsAnotherAffix(affix, previousAffix, !prefix, prefixId);
    if (!skipLookup
        && !processor.processStemCandidate(
            word, offset, length, affix, outerPrefix, innerPrefix, outerSuffix, innerSuffix)) {
      return false;
    }

    if (innerSuffix >= 0) return true;

    int recursionDepth =
        (outerSuffix >= 0 ? 1 : 0) + (innerPrefix >= 0 ? 2 : outerPrefix >= 0 ? 1 : 0) - 1;
    if (dictionary.isCrossProduct(affix) && recursionDepth <= 1) {
      char flag = dictionary.affixData(affix, Dictionary.AFFIX_FLAG);
      boolean doPrefix;
      if (recursionDepth == 0) {
        if (prefix) {
          doPrefix = dictionary.complexPrefixes && dictionary.isSecondStagePrefix(flag);
          // we took away the first prefix.
          // COMPLEXPREFIXES = true:  combine with a second prefix and another suffix
          // COMPLEXPREFIXES = false: combine with a suffix
        } else if (!dictionary.complexPrefixes && dictionary.isSecondStageSuffix(flag)) {
          doPrefix = false;
          // we took away a suffix.
          // COMPLEXPREFIXES = true: we don't recurse! only one suffix allowed
          // COMPLEXPREFIXES = false: combine with another suffix
        } else {
          return true;
        }
      } else {
        if (prefix && dictionary.complexPrefixes) {
          doPrefix = true;
          // we took away the second prefix: go look for another suffix
        } else if (prefix || dictionary.complexPrefixes || !dictionary.isSecondStageSuffix(flag)) {
          return true;
        } else {
          // we took away a prefix, then a suffix: go look for another suffix
          doPrefix = false;
        }
      }

      return removeAffixes(
          word, offset, length, doPrefix, outerPrefix, innerPrefix, outerSuffix, processor);
    }

    return true;
  }

  abstract static class StemCandidateProcessor {
    private final WordContext context;

    StemCandidateProcessor(WordContext context) {
      this.context = context;
    }

    abstract boolean processStemCandidate(
        char[] word,
        int offset,
        int length,
        int lastAffix,
        int outerPrefix,
        int innerPrefix,
        int outerSuffix,
        int innerSuffix);
  }

  private boolean isRootCompatibleWithContext(WordContext context, int lastAffix, int entryId) {
    if (!context.isCompound() && dictionary.hasFlag(entryId, dictionary.onlyincompound)) {
      return false;
    }
    if (context.isCompound() && context != WordContext.COMPOUND_RULE_END) {
      char cFlag = context.requiredFlag(dictionary);
      return dictionary.hasFlag(entryId, cFlag)
          || dictionary.isFlagAppendedByAffix(lastAffix, cFlag)
          || dictionary.hasFlag(entryId, dictionary.compoundFlag)
          || dictionary.isFlagAppendedByAffix(lastAffix, dictionary.compoundFlag);
    }
    return true;
  }

  private int morphDataId(IntsRef forms, int i) {
    return dictionary.hasCustomMorphData ? forms.ints[forms.offset + i + 1] : 0;
  }

  private boolean needsAnotherAffix(int affix, int previousAffix, boolean isSuffix, int prefixId) {
    char circumfix = dictionary.circumfix;
    // if circumfix was previously set by a prefix, we must check this suffix,
    // to ensure it has it, and vice versa
    if (isSuffix) {
      if (dictionary.isFlagAppendedByAffix(prefixId, circumfix)
          != dictionary.isFlagAppendedByAffix(affix, circumfix)) {
        return true;
      }
    }
    if (dictionary.isFlagAppendedByAffix(affix, dictionary.needaffix)) {
      return !isSuffix
          || previousAffix < 0
          || dictionary.isFlagAppendedByAffix(previousAffix, dictionary.needaffix);
    }
    return false;
  }
}
