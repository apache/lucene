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

import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_APPEND;
import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_FLAG;
import static org.apache.lucene.analysis.hunspell.Dictionary.FLAG_UNSET;
import static org.apache.lucene.analysis.hunspell.Dictionary.toSortedCharArray;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.hunspell.AffixedWord.Affix;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.IntsRefFSTEnum;

/**
 * A utility class used for generating possible word forms by adding affixes to stems ({@link
 * #getAllWordForms(String, String, Runnable)}), and suggesting stems and flags to generate the
 * given set of words ({@link #compress(List, Set, Runnable)}).
 */
public class WordFormGenerator {
  private final Dictionary dictionary;
  private final Map<Character, List<AffixEntry>> affixes = new HashMap<>();
  private final Stemmer stemmer;

  public WordFormGenerator(Dictionary dictionary) {
    this.dictionary = dictionary;
    fillAffixMap(dictionary.prefixes, AffixKind.PREFIX);
    fillAffixMap(dictionary.suffixes, AffixKind.SUFFIX);
    stemmer = new Stemmer(dictionary);
  }

  private void fillAffixMap(FST<IntsRef> fst, AffixKind kind) {
    if (fst == null) return;

    IntsRefFSTEnum<IntsRef> fstEnum = new IntsRefFSTEnum<>(fst);
    try {
      while (true) {
        IntsRefFSTEnum.InputOutput<IntsRef> io = fstEnum.next();
        if (io == null) break;

        IntsRef affixIds = io.output;
        for (int j = 0; j < affixIds.length; j++) {
          int id = affixIds.ints[affixIds.offset + j];
          char flag = dictionary.affixData(id, AFFIX_FLAG);
          var entry =
              new AffixEntry(id, flag, kind, toString(kind, io.input), strip(id), condition(id));
          affixes.computeIfAbsent(flag, __ -> new ArrayList<>()).add(entry);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private String toString(AffixKind kind, IntsRef input) {
    char[] affixChars = new char[input.length];
    for (int i = 0; i < affixChars.length; i++) {
      affixChars[kind == AffixKind.PREFIX ? i : affixChars.length - i - 1] =
          (char) input.ints[input.offset + i];
    }
    return new String(affixChars);
  }

  private AffixCondition condition(int affixId) {
    int condition = dictionary.getAffixCondition(affixId);
    return condition == 0 ? AffixCondition.ALWAYS_TRUE : dictionary.patterns.get(condition);
  }

  private String strip(int affixId) {
    int stripOrd = dictionary.affixData(affixId, Dictionary.AFFIX_STRIP_ORD);
    int stripStart = dictionary.stripOffsets[stripOrd];
    int stripEnd = dictionary.stripOffsets[stripOrd + 1];
    return new String(dictionary.stripData, stripStart, stripEnd - stripStart);
  }

  /**
   * Generate all word forms for all dictionary entries with the given root word. The result order
   * is stable but not specified. This is equivalent to "unmunch" from the "hunspell-tools" package.
   *
   * @param checkCanceled an object that's periodically called, allowing to interrupt the generation
   *     by throwing an exception
   */
  public List<AffixedWord> getAllWordForms(String root, Runnable checkCanceled) {
    List<AffixedWord> result = new ArrayList<>();
    DictEntries entries = dictionary.lookupEntries(root);
    if (entries != null) {
      for (DictEntry entry : entries) {
        result.addAll(getAllWordForms(root, entry.getFlags(), checkCanceled));
      }
    }
    return result;
  }

  /**
   * Generate all word forms for the given root pretending it has the given flags (in the same
   * format as the dictionary uses). The result order is stable but not specified. This is
   * equivalent to "unmunch" from the "hunspell-tools" package.
   *
   * @param checkCanceled an object that's periodically called, allowing to interrupt the generation
   *     by throwing an exception
   */
  public List<AffixedWord> getAllWordForms(String stem, String flags, Runnable checkCanceled) {
    var encodedFlags = dictionary.flagParsingStrategy.parseUtfFlags(flags);
    if (!shouldConsiderAtAll(encodedFlags)) return List.of();

    return getAllWordForms(DictEntry.create(stem, flags), encodedFlags, checkCanceled);
  }

  private List<AffixedWord> getAllWordForms(
      DictEntry entry, char[] encodedFlags, Runnable checkCanceled) {
    encodedFlags = sortAndDeduplicate(encodedFlags);
    List<AffixedWord> result = new ArrayList<>();
    AffixedWord bare = new AffixedWord(entry.getStem(), entry, List.of(), List.of());
    checkCanceled.run();
    if (!FlagEnumerator.hasFlagInSortedArray(
        dictionary.needaffix, encodedFlags, 0, encodedFlags.length)) {
      result.add(bare);
    }
    result.addAll(expand(bare, encodedFlags, checkCanceled));
    return result;
  }

  private static char[] sortAndDeduplicate(char[] flags) {
    Arrays.sort(flags);
    for (int i = 1; i < flags.length; i++) {
      if (flags[i] == flags[i - 1]) {
        return deduplicate(flags);
      }
    }
    return flags;
  }

  private static char[] deduplicate(char[] flags) {
    Set<Character> set = new HashSet<>();
    for (char flag : flags) {
      set.add(flag);
    }
    return toSortedCharArray(set);
  }

  /**
   * A sanity-check that the word form generated by affixation in {@link #getAllWordForms(String,
   * String, Runnable)} is indeed accepted by the spell-checker and analyzed to be the form of the
   * original dictionary entry. This can be overridden for cases where such check is unnecessary or
   * can be done more efficiently.
   */
  protected boolean canStemToOriginal(AffixedWord derived) {
    String word = derived.getWord();
    char[] chars = word.toCharArray();
    if (isForbiddenWord(chars, 0, chars.length)) {
      return false;
    }

    String stem = derived.getDictEntry().getStem();
    var processor =
        new Stemmer.StemCandidateProcessor(WordContext.SIMPLE_WORD) {
          boolean foundStem = false;
          boolean foundForbidden = false;

          @Override
          boolean processStemCandidate(
              char[] chars,
              int offset,
              int length,
              int lastAffix,
              int outerPrefix,
              int innerPrefix,
              int outerSuffix,
              int innerSuffix) {
            if (isForbiddenWord(chars, offset, length)) {
              foundForbidden = true;
              return false;
            }
            foundStem |= length == stem.length() && stem.equals(new String(chars, offset, length));
            return !foundStem;
          }
        };
    stemmer.removeAffixes(chars, 0, chars.length, true, -1, -1, -1, processor);
    return processor.foundStem && !processor.foundForbidden;
  }

  private boolean isForbiddenWord(char[] chars, int offset, int length) {
    if (dictionary.forbiddenword != FLAG_UNSET) {
      IntsRef forms = dictionary.lookupWord(chars, offset, length);
      if (forms != null) {
        for (int i = 0; i < forms.length; i += dictionary.formStep()) {
          if (dictionary.hasFlag(forms.ints[forms.offset + i], dictionary.forbiddenword)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private List<AffixedWord> expand(AffixedWord stem, char[] flags, Runnable checkCanceled) {
    List<AffixedWord> result = new ArrayList<>();
    for (char flag : flags) {
      List<AffixEntry> entries = affixes.get(flag);
      if (entries == null) continue;

      AffixKind kind = entries.get(0).kind;
      if (!isCompatibleWithPreviousAffixes(stem, kind, flag)) continue;

      for (AffixEntry affix : entries) {
        checkCanceled.run();
        AffixedWord derived = affix.apply(stem, dictionary);
        if (derived != null) {
          char[] append = appendFlags(affix);
          if (shouldConsiderAtAll(append)) {
            if (canStemToOriginal(derived)) {
              result.add(derived);
            }
            if (dictionary.isCrossProduct(affix.id)) {
              result.addAll(expand(derived, updateFlags(flags, flag, append), checkCanceled));
            }
          }
        }
      }
    }
    return result;
  }

  private boolean shouldConsiderAtAll(char[] flags) {
    for (char flag : flags) {
      if (flag == dictionary.compoundBegin
          || flag == dictionary.compoundMiddle
          || flag == dictionary.compoundEnd
          || flag == dictionary.forbiddenword
          || flag == dictionary.onlyincompound) {
        return false;
      }
    }

    return true;
  }

  private char[] updateFlags(char[] flags, char toRemove, char[] toAppend) {
    char[] result = new char[flags.length + toAppend.length - 1];
    int index = 0;
    for (char flag : flags) {
      if (flag != toRemove && flag != dictionary.needaffix) {
        result[index++] = flag;
      }
    }
    for (char flag : toAppend) {
      result[index++] = flag;
    }
    return sortAndDeduplicate(result);
  }

  private char[] appendFlags(AffixEntry affix) {
    char appendId = dictionary.affixData(affix.id, AFFIX_APPEND);
    return appendId == 0 ? new char[0] : dictionary.flagLookup.getFlags(appendId);
  }

  /**
   * Traverse the whole dictionary and derive all word forms via affixation (as in {@link
   * #getAllWordForms(String, String, Runnable)}) for each of the entries. The iteration order is
   * undefined. Only "simple" words are returned, no compounding flags are processed. Upper- and
   * title-case variations are not returned, even if the spellchecker accepts them.
   *
   * @param consumer the object that receives each derived word form
   * @param checkCanceled an object that's periodically called, allowing to interrupt the traversal
   *     and generation by throwing an exception
   */
  public void generateAllSimpleWords(Consumer<AffixedWord> consumer, Runnable checkCanceled) {
    dictionary.words.processAllWords(
        1,
        Integer.MAX_VALUE,
        false,
        (root, lazyForms) -> {
          String rootStr = root.toString();
          IntsRef forms = lazyForms.get();
          for (int i = 0; i < forms.length; i += dictionary.formStep()) {
            char[] encodedFlags = dictionary.flagLookup.getFlags(forms.ints[forms.offset + i]);
            if (shouldConsiderAtAll(encodedFlags)) {
              String presentableFlags = dictionary.flagParsingStrategy.printFlags(encodedFlags);
              DictEntry entry = DictEntry.create(rootStr, presentableFlags);
              for (AffixedWord aw : getAllWordForms(entry, encodedFlags, checkCanceled)) {
                consumer.accept(aw);
              }
            }
          }
        });
  }

  /**
   * Given a list of words, try to produce a smaller set of dictionary entries (with some flags)
   * that would generate these words. This is equivalent to "munch" from the "hunspell-tools"
   * package. The algorithm tries to minimize the number of the dictionary entries to add or change,
   * the number of flags involved, and the number of non-requested additionally generated words. All
   * the mentioned words are in the dictionary format and case: no ICONV/OCONV/IGNORE conversions
   * are applied.
   *
   * @param words the list of words to generate
   * @param forbidden the set of words to avoid generating
   * @param checkCanceled an object that's periodically called, allowing to interrupt the generation
   *     by throwing an exception
   * @return the information about suggested dictionary entries and overgenerated words, or {@code
   *     null} if the algorithm couldn't generate anything
   */
  public EntrySuggestion compress(
      List<String> words, Set<String> forbidden, Runnable checkCanceled) {
    if (words.isEmpty()) return null;
    if (words.stream().anyMatch(forbidden::contains)) {
      throw new IllegalArgumentException("'words' and 'forbidden' shouldn't intersect");
    }

    return new WordCompressor(words, forbidden, checkCanceled).compress();
  }

  private static class AffixEntry {
    final int id;
    final char flag;
    final AffixKind kind;
    final String affix;
    final String strip;
    final AffixCondition condition;

    AffixEntry(
        int id, char flag, AffixKind kind, String affix, String strip, AffixCondition condition) {
      this.id = id;
      this.flag = flag;
      this.kind = kind;
      this.affix = affix;
      this.strip = strip;
      this.condition = condition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof AffixEntry)) return false;
      AffixEntry that = (AffixEntry) o;
      return id == that.id
          && flag == that.flag
          && kind == that.kind
          && affix.equals(that.affix)
          && strip.equals(that.strip)
          && condition.equals(that.condition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, flag, kind, affix, strip, condition);
    }

    AffixedWord apply(AffixedWord stem, Dictionary dictionary) {
      String word = stem.getWord();
      boolean isPrefix = kind == AffixKind.PREFIX;
      if (!(isPrefix ? word.startsWith(strip) : word.endsWith(strip))) return null;

      String stripped =
          isPrefix
              ? word.substring(strip.length())
              : word.substring(0, word.length() - strip.length());
      if (!condition.acceptsStem(stripped)) return null;

      String applied = isPrefix ? affix + stripped : stripped + affix;
      List<Affix> prefixes = isPrefix ? new ArrayList<>(stem.getPrefixes()) : stem.getPrefixes();
      List<Affix> suffixes = isPrefix ? stem.getSuffixes() : new ArrayList<>(stem.getSuffixes());
      (isPrefix ? prefixes : suffixes).add(0, new Affix(dictionary, id));
      return new AffixedWord(applied, stem.getDictEntry(), prefixes, suffixes);
    }
  }

  private boolean isCompatibleWithPreviousAffixes(AffixedWord stem, AffixKind kind, char flag) {
    boolean isPrefix = kind == AffixKind.PREFIX;
    List<Affix> sameAffixes = isPrefix ? stem.getPrefixes() : stem.getSuffixes();
    int size = sameAffixes.size();
    if (size == 2) return false;
    if (isPrefix && size == 1 && !dictionary.complexPrefixes) return false;
    if (!isPrefix && !stem.getPrefixes().isEmpty()) return false;
    if (size == 1 && !dictionary.isFlagAppendedByAffix(sameAffixes.get(0).affixId, flag)) {
      return false;
    }
    return true;
  }

  private class WordCompressor {
    private final Comparator<State> solutionFitness =
        Comparator.comparingInt((State s) -> s.forbidden)
            .thenComparingInt(s -> s.underGenerated)
            .thenComparingInt(s -> s.stemToFlags.size())
            .thenComparingInt(s -> s.overGenerated);
    private final Set<String> forbidden;
    private final Runnable checkCanceled;
    private final Set<String> wordSet;
    private final Set<String> existingStems;
    private final Map<String, Set<FlagSet>> stemToPossibleFlags = new HashMap<>();
    private final Map<String, Integer> stemCounts = new LinkedHashMap<>();

    WordCompressor(List<String> words, Set<String> forbidden, Runnable checkCanceled) {
      this.forbidden = forbidden;
      this.checkCanceled = checkCanceled;
      wordSet = new HashSet<>(words);

      Stemmer.StemCandidateProcessor processor =
          new Stemmer.StemCandidateProcessor(WordContext.SIMPLE_WORD) {
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
              String candidate = new String(word, offset, length);
              stemCounts.merge(candidate, 1, Integer::sum);
              Set<Character> flags = new LinkedHashSet<>();
              if (outerPrefix >= 0) flags.add(dictionary.affixData(outerPrefix, AFFIX_FLAG));
              if (innerPrefix >= 0) flags.add(dictionary.affixData(innerPrefix, AFFIX_FLAG));
              if (outerSuffix >= 0) flags.add(dictionary.affixData(outerSuffix, AFFIX_FLAG));
              if (innerSuffix >= 0) flags.add(dictionary.affixData(innerSuffix, AFFIX_FLAG));
              stemToPossibleFlags
                  .computeIfAbsent(candidate, __ -> new LinkedHashSet<>())
                  .add(new FlagSet(flags, dictionary));
              return true;
            }
          };

      for (String word : words) {
        checkCanceled.run();
        stemCounts.merge(word, 1, Integer::sum);
        stemToPossibleFlags.computeIfAbsent(word, __ -> new LinkedHashSet<>());
        stemmer.removeAffixes(word.toCharArray(), 0, word.length(), true, -1, -1, -1, processor);
      }

      existingStems =
          stemCounts.keySet().stream()
              .filter(stem -> dictionary.lookupEntries(stem) != null)
              .collect(Collectors.toSet());
    }

    EntrySuggestion compress() {
      Comparator<String> stemSorter =
          Comparator.comparing((String s) -> existingStems.contains(s))
              .thenComparing(stemCounts::get)
              .reversed();
      List<String> sortedStems =
          stemCounts.keySet().stream().sorted(stemSorter).collect(Collectors.toList());
      PriorityQueue<State> queue = new PriorityQueue<>(solutionFitness);
      queue.offer(new State(Map.of(), wordSet.size(), 0, 0));
      State result = null;
      while (!queue.isEmpty()) {
        State state = queue.poll();
        if (state.underGenerated == 0) {
          if (result == null || solutionFitness.compare(state, result) < 0) result = state;
          if (state.forbidden == 0) break;
          continue;
        }

        for (String stem : sortedStems) {
          if (!state.stemToFlags.containsKey(stem)) {
            queue.offer(addStem(state, stem));
          }
        }

        for (Map.Entry<String, Set<FlagSet>> entry : state.stemToFlags.entrySet()) {
          for (FlagSet flags : stemToPossibleFlags.get(entry.getKey())) {
            if (!entry.getValue().contains(flags)) {
              queue.offer(addFlags(state, entry.getKey(), flags));
            }
          }
        }
      }
      return result == null ? null : toSuggestion(result);
    }

    EntrySuggestion toSuggestion(State state) {
      List<DictEntry> toEdit = new ArrayList<>();
      List<DictEntry> toAdd = new ArrayList<>();
      for (Map.Entry<String, Set<FlagSet>> entry : state.stemToFlags.entrySet()) {
        addEntry(toEdit, toAdd, entry.getKey(), FlagSet.flatten(entry.getValue()));
      }

      List<String> extraGenerated = new ArrayList<>();
      for (String extra :
          allGenerated(state.stemToFlags).distinct().sorted().collect(Collectors.toList())) {
        if (wordSet.contains(extra)) continue;

        if (forbidden.contains(extra) && dictionary.forbiddenword != FLAG_UNSET) {
          addEntry(toEdit, toAdd, extra, Set.of(dictionary.forbiddenword));
        } else {
          extraGenerated.add(extra);
        }
      }

      return new EntrySuggestion(toEdit, toAdd, extraGenerated);
    }

    private void addEntry(
        List<DictEntry> toEdit, List<DictEntry> toAdd, String stem, Set<Character> flags) {
      String flagString = toFlagString(flags);
      (existingStems.contains(stem) ? toEdit : toAdd).add(DictEntry.create(stem, flagString));
    }

    private State addStem(State state, String stem) {
      LinkedHashMap<String, Set<FlagSet>> stemToFlags = new LinkedHashMap<>(state.stemToFlags);
      stemToFlags.put(stem, Set.of());
      return newState(stemToFlags);
    }

    private State addFlags(State state, String stem, FlagSet flags) {
      LinkedHashMap<String, Set<FlagSet>> stemToFlags = new LinkedHashMap<>(state.stemToFlags);
      Set<FlagSet> flagSets = new LinkedHashSet<>(stemToFlags.get(stem));
      flagSets.add(flags);
      stemToFlags.put(stem, flagSets);
      return newState(stemToFlags);
    }

    private State newState(Map<String, Set<FlagSet>> stemToFlags) {
      Set<String> allGenerated = allGenerated(stemToFlags).collect(Collectors.toSet());
      return new State(
          stemToFlags,
          (int) wordSet.stream().filter(s -> !allGenerated.contains(s)).count(),
          (int) allGenerated.stream().filter(s -> !wordSet.contains(s)).count(),
          (int) allGenerated.stream().filter(s -> forbidden.contains(s)).count());
    }

    private final Map<StemWithFlags, List<String>> expansionCache = new HashMap<>();

    private class StemWithFlags {
      final String stem;
      final Set<FlagSet> flags;

      StemWithFlags(String stem, Set<FlagSet> flags) {
        this.stem = stem;
        this.flags = flags;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StemWithFlags)) return false;
        StemWithFlags that = (StemWithFlags) o;
        return stem.equals(that.stem) && flags.equals(that.flags);
      }

      @Override
      public int hashCode() {
        return Objects.hash(stem, flags);
      }
    }

    private Stream<String> allGenerated(Map<String, Set<FlagSet>> stemToFlags) {
      Function<StemWithFlags, List<String>> expandToWords =
          e ->
              expand(e.stem, FlagSet.flatten(e.flags)).stream()
                  .map(w -> w.getWord())
                  .collect(Collectors.toList());
      return stemToFlags.entrySet().stream()
          .map(e -> new StemWithFlags(e.getKey(), e.getValue()))
          .flatMap(swc -> expansionCache.computeIfAbsent(swc, expandToWords).stream());
    }

    private List<AffixedWord> expand(String stem, Set<Character> flagSet) {
      return getAllWordForms(stem, toFlagString(flagSet), checkCanceled);
    }

    private String toFlagString(Set<Character> flagSet) {
      return dictionary.flagParsingStrategy.printFlags(Dictionary.toSortedCharArray(flagSet));
    }
  }

  private static class FlagSet {
    final Set<Character> flags;
    final Dictionary dictionary;

    FlagSet(Set<Character> flags, Dictionary dictionary) {
      this.flags = flags;
      this.dictionary = dictionary;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FlagSet)) return false;
      FlagSet flagSet = (FlagSet) o;
      return flags.equals(flagSet.flags) && dictionary.equals(flagSet.dictionary);
    }

    @Override
    public int hashCode() {
      return Objects.hash(flags, dictionary);
    }

    static Set<Character> flatten(Set<FlagSet> flagSets) {
      return flagSets.stream().flatMap(f -> f.flags.stream()).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
      return dictionary.flagParsingStrategy.printFlags(Dictionary.toSortedCharArray(flags));
    }
  }

  private static class State {
    final Map<String, Set<FlagSet>> stemToFlags;
    final int underGenerated;
    final int overGenerated;
    final int forbidden;

    State(
        Map<String, Set<FlagSet>> stemToFlags,
        int underGenerated,
        int overGenerated,
        int forbidden) {
      this.stemToFlags = stemToFlags;
      this.underGenerated = underGenerated;
      this.overGenerated = overGenerated;
      this.forbidden = forbidden;
    }
  }
}
