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

import static org.apache.lucene.analysis.hunspell.AffixKind.PREFIX;
import static org.apache.lucene.analysis.hunspell.AffixKind.SUFFIX;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.hunspell.SortingStrategy.EntryAccumulator;
import org.apache.lucene.analysis.hunspell.SortingStrategy.EntrySupplier;
import org.apache.lucene.internal.hppc.CharHashSet;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.IntSequenceOutputs;
import org.apache.lucene.util.fst.Util;

/** In-memory structure for the dictionary (.dic) and affix (.aff) data of a hunspell dictionary. */
public class Dictionary {
  // Derived from woorm/LibreOffice dictionaries.
  // See TestAllDictionaries.testMaxPrologueNeeded.
  static final int MAX_PROLOGUE_SCAN_WINDOW = 30 * 1024;

  static final char[] NOFLAGS = new char[0];

  static final char FLAG_UNSET = (char) 0;
  private static final int DEFAULT_FLAGS = 65510;
  static final char HIDDEN_FLAG = (char) 65511; // called 'ONLYUPCASEFLAG' in Hunspell

  static final Charset DEFAULT_CHARSET = StandardCharsets.ISO_8859_1;
  CharsetDecoder decoder = replacingDecoder(DEFAULT_CHARSET);

  FST<IntsRef> prefixes;
  FST<IntsRef> suffixes;
  Breaks breaks = Breaks.DEFAULT;

  /**
   * All condition checks used by prefixes and suffixes. these are typically re-used across many
   * affix stripping rules. so these are deduplicated, to save RAM.
   */
  ArrayList<AffixCondition> patterns = new ArrayList<>();

  /** The entries in the .dic file, mapping to their set of flags */
  WordStorage words;

  /**
   * The list of unique flagsets (wordforms). theoretically huge, but practically small (for Polish
   * this is 756), otherwise humans wouldn't be able to deal with it either.
   */
  final FlagEnumerator.Lookup flagLookup;

  // the list of unique strip affixes.
  char[] stripData;
  int[] stripOffsets;

  String wordChars = "";

  // 4 chars per affix, each char representing an unsigned 2-byte integer
  char[] affixData = new char[32];
  private int currentAffix = 0;

  // offsets in affixData
  static final int AFFIX_FLAG = 0;
  static final int AFFIX_STRIP_ORD = 1;
  private static final int AFFIX_CONDITION = 2;
  static final int AFFIX_APPEND = 3;

  // Default flag parsing strategy
  FlagParsingStrategy flagParsingStrategy = new SimpleFlagParsingStrategy();

  // AF entries
  private String[] aliases;
  private int aliasCount = 0;

  // AM entries
  private String[] morphAliases;
  private int morphAliasCount = 0;

  final List<String> morphData = new ArrayList<>(Collections.singletonList("")); // empty data at 0

  /**
   * we set this during sorting, so we know to add an extra int (index in {@link #morphData}) to FST
   * output
   */
  boolean hasCustomMorphData;

  boolean ignoreCase;
  boolean checkSharpS;
  boolean complexPrefixes;

  /**
   * All flags used in affix continuation classes. If an outer affix's flag isn't here, there's no
   * need to do 2-level affix stripping with it.
   */
  private char[] secondStagePrefixFlags, secondStageSuffixFlags;

  char circumfix;
  char keepcase, forceUCase;
  char needaffix;
  char forbiddenword;
  char onlyincompound, compoundBegin, compoundMiddle, compoundEnd, compoundFlag;
  char compoundPermit, compoundForbid;
  boolean checkCompoundCase, checkCompoundDup, checkCompoundRep;
  boolean checkCompoundTriple, simplifiedTriple;
  int compoundMin = 3, compoundMax = Integer.MAX_VALUE;
  CompoundRule[] compoundRules; // nullable
  List<CheckCompoundPattern> checkCompoundPatterns = new ArrayList<>();

  // ignored characters (dictionary, affix, inputs)
  private char[] ignore;

  String tryChars = "";
  String[] neighborKeyGroups = {"qwertyuiop", "asdfghjkl", "zxcvbnm"};
  boolean enableSplitSuggestions = true;
  List<RepEntry> repTable = new ArrayList<>();
  List<List<String>> mapTable = new ArrayList<>();
  int maxDiff = 5;
  int maxNGramSuggestions = 4;
  boolean onlyMaxDiff;
  char noSuggest, subStandard;
  ConvTable iconv, oconv;

  // true if we can strip suffixes "down to nothing"
  boolean fullStrip;

  // language declaration of the dictionary
  String language;
  // true if case algorithms should use alternate (Turkish/Azeri) mapping
  private boolean alternateCasing;

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to
   * hunspell affix and dictionary files. You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionary InputStream for reading the hunspell dictionary file (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(
      Directory tempDir, String tempFileNamePrefix, InputStream affix, InputStream dictionary)
      throws IOException, ParseException {
    this(tempDir, tempFileNamePrefix, affix, Collections.singletonList(dictionary), false);
  }

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to
   * hunspell affix and dictionary files. You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionaries InputStream for reading the hunspell dictionary files (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(
      Directory tempDir,
      String tempFileNamePrefix,
      InputStream affix,
      List<InputStream> dictionaries,
      boolean ignoreCase)
      throws IOException, ParseException {
    this(affix, dictionaries, ignoreCase, SortingStrategy.offline(tempDir, tempFileNamePrefix));
  }

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to
   * hunspell affix and dictionary files. You have to close the provided InputStreams yourself.
   *
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionaries InputStream for reading the hunspell dictionary files (won't be closed).
   * @param sortingStrategy the entry strategy for the dictionary loading
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(
      InputStream affix,
      List<InputStream> dictionaries,
      boolean ignoreCase,
      SortingStrategy sortingStrategy)
      throws IOException, ParseException {
    this.ignoreCase = ignoreCase;

    try (BufferedInputStream affixStream =
        new BufferedInputStream(affix, MAX_PROLOGUE_SCAN_WINDOW) {
          @Override
          public void close() {
            // TODO: maybe we should consume and close it? Why does it need to stay open?
            // Don't close the affix stream as per javadoc.
          }
        }) {
      // I assume we don't support other BOMs (utf16, etc.)? We trivially could,
      // by adding maybeConsume() with a proper bom... but I don't see hunspell repo to have
      // any such exotic examples.
      Charset streamCharset;
      if (maybeConsume(affixStream, BOM_UTF8)) {
        streamCharset = StandardCharsets.UTF_8;
      } else {
        streamCharset = DEFAULT_CHARSET;
      }

      /*
       * pass 1: look for encoding & flag. This is simple but works. We just prefetch
       * a large enough chunk of the input and scan through it. The buffered data will
       * be subsequently reused anyway so nothing is wasted.
       */
      affixStream.mark(MAX_PROLOGUE_SCAN_WINDOW);
      byte[] prologue = affixStream.readNBytes(MAX_PROLOGUE_SCAN_WINDOW - 1);
      affixStream.reset();
      readConfig(new ByteArrayInputStream(prologue), streamCharset);

      // pass 2: parse affixes
      FlagEnumerator flagEnumerator = new FlagEnumerator();
      readAffixFile(affixStream, decoder, flagEnumerator);

      // read dictionary entries
      EntryAccumulator acc = sortingStrategy.start();
      mergeDictionaries(dictionaries, decoder, acc);
      try (EntrySupplier sorted = acc.finishAndSort()) {
        words = readSortedDictionaries(flagEnumerator, sorted);
      }
      flagLookup = flagEnumerator.finish();
      aliases = null; // no longer needed
      morphAliases = null; // no longer needed
    }
  }

  int formStep() {
    return hasCustomMorphData ? 2 : 1;
  }

  /** Looks up Hunspell word forms from the dictionary */
  IntsRef lookupWord(char[] word, int offset, int length) {
    return words.lookupWord(word, offset, length);
  }

  // only for testing
  IntsRef lookupPrefix(char[] word) {
    return lookup(prefixes, word);
  }

  // only for testing
  IntsRef lookupSuffix(char[] word) {
    return lookup(suffixes, word);
  }

  private IntsRef lookup(FST<IntsRef> fst, char[] word) {
    final FST.BytesReader bytesReader = fst.getBytesReader();
    final FST.Arc<IntsRef> arc = fst.getFirstArc(new FST.Arc<>());
    // Accumulate output as we go
    IntsRef output = fst.outputs.getNoOutput();

    for (int i = 0, cp; i < word.length; i += Character.charCount(cp)) {
      cp = Character.codePointAt(word, i, word.length);
      output = nextArc(fst, arc, bytesReader, output, cp);
      if (output == null) {
        return null;
      }
    }
    return nextArc(fst, arc, bytesReader, output, FST.END_LABEL);
  }

  static IntsRef nextArc(
      FST<IntsRef> fst, FST.Arc<IntsRef> arc, FST.BytesReader reader, IntsRef output, int ch) {
    try {
      if (fst.findTargetArc(ch, arc, arc, reader) == null) {
        return null;
      }
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    return fst.outputs.add(output, arc.output());
  }

  /**
   * Reads the affix file through the provided InputStream, building up the prefix and suffix maps
   *
   * @param affixStream InputStream to read the content of the affix file from
   * @param decoder CharsetDecoder to decode the content of the file
   * @throws IOException Can be thrown while reading from the InputStream
   */
  private void readAffixFile(InputStream affixStream, CharsetDecoder decoder, FlagEnumerator flags)
      throws IOException, ParseException {
    TreeMap<String, IntArrayList> prefixes = new TreeMap<>();
    TreeMap<String, IntArrayList> suffixes = new TreeMap<>();
    CharHashSet prefixContFlags = new CharHashSet();
    CharHashSet suffixContFlags = new CharHashSet();
    Map<String, Integer> seenPatterns = new HashMap<>();

    // zero condition -> 0 ord
    seenPatterns.put(AffixCondition.ALWAYS_TRUE_KEY, 0);
    patterns.add(null);

    // zero strip -> 0 ord
    Map<String, Integer> seenStrips = new LinkedHashMap<>();
    seenStrips.put("", 0);

    LineNumberReader reader = new LineNumberReader(new InputStreamReader(affixStream, decoder));
    String line;
    while ((line = reader.readLine()) != null) {
      // ignore any BOM marker on first line
      if (reader.getLineNumber() == 1 && line.startsWith("\uFEFF")) {
        line = line.substring(1);
      }
      line = line.trim();
      if (line.isEmpty()) continue;

      String firstWord = line.split("\\s")[0];
      // TODO: convert to a switch?
      if ("AF".equals(firstWord)) {
        parseAlias(line);
      } else if ("AM".equals(firstWord)) {
        parseMorphAlias(line);
      } else if ("PFX".equals(firstWord)) {
        parseAffix(
            prefixes, prefixContFlags, line, reader, PREFIX, seenPatterns, seenStrips, flags);
      } else if ("SFX".equals(firstWord)) {
        parseAffix(
            suffixes, suffixContFlags, line, reader, SUFFIX, seenPatterns, seenStrips, flags);
      } else if (line.equals("COMPLEXPREFIXES")) {
        complexPrefixes =
            true; // 2-stage prefix+1-stage suffix instead of 2-stage suffix+1-stage prefix
      } else if ("CIRCUMFIX".equals(firstWord)) {
        circumfix = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("KEEPCASE".equals(firstWord)) {
        keepcase = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("FORCEUCASE".equals(firstWord)) {
        forceUCase = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("NEEDAFFIX".equals(firstWord) || "PSEUDOROOT".equals(firstWord)) {
        needaffix = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("ONLYINCOMPOUND".equals(firstWord)) {
        onlyincompound = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("CHECKSHARPS".equals(firstWord)) {
        checkSharpS = true;
      } else if ("IGNORE".equals(firstWord)) {
        ignore = singleArgument(reader, line).toCharArray();
        Arrays.sort(ignore);
      } else if ("ICONV".equals(firstWord) || "OCONV".equals(firstWord)) {
        int num = parseNum(reader, line);
        ConvTable res = parseConversions(reader, num);
        if (line.startsWith("I")) {
          iconv = res;
        } else {
          oconv = res;
        }
      } else if ("FULLSTRIP".equals(firstWord)) {
        fullStrip = true;
      } else if ("LANG".equals(firstWord)) {
        language = singleArgument(reader, line);
        this.alternateCasing = hasLanguage("tr", "az");
      } else if ("BREAK".equals(firstWord)) {
        breaks = parseBreaks(reader, line);
      } else if ("WORDCHARS".equals(firstWord)) {
        wordChars = firstArgument(reader, line);
      } else if ("TRY".equals(firstWord)) {
        tryChars = firstArgument(reader, line);
      } else if ("REP".equals(firstWord)) {
        int count = parseNum(reader, line);
        for (int i = 0; i < count; i++) {
          String[] parts = splitBySpace(reader, reader.readLine(), 3, Integer.MAX_VALUE);
          repTable.add(new RepEntry(parts[1], parts[2]));
        }
      } else if ("MAP".equals(firstWord)) {
        int count = parseNum(reader, line);
        for (int i = 0; i < count; i++) {
          mapTable.add(parseMapEntry(reader, reader.readLine()));
        }
      } else if ("KEY".equals(firstWord)) {
        neighborKeyGroups = singleArgument(reader, line).split("\\|");
      } else if ("NOSPLITSUGS".equals(firstWord)) {
        enableSplitSuggestions = false;
      } else if ("MAXNGRAMSUGS".equals(firstWord)) {
        maxNGramSuggestions = Integer.parseInt(singleArgument(reader, line));
      } else if ("MAXDIFF".equals(firstWord)) {
        int i = Integer.parseInt(singleArgument(reader, line));
        if (i < 0 || i > 10) {
          throw new ParseException("MAXDIFF should be between 0 and 10", reader.getLineNumber());
        }
        maxDiff = i;
      } else if ("ONLYMAXDIFF".equals(firstWord)) {
        onlyMaxDiff = true;
      } else if ("FORBIDDENWORD".equals(firstWord)) {
        forbiddenword = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("NOSUGGEST".equals(firstWord)) {
        noSuggest = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("SUBSTANDARD".equals(firstWord)) {
        subStandard = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDMIN".equals(firstWord)) {
        compoundMin = Math.max(1, parseNum(reader, line));
      } else if ("COMPOUNDWORDMAX".equals(firstWord)) {
        compoundMax = Math.max(1, parseNum(reader, line));
      } else if ("COMPOUNDRULE".equals(firstWord)) {
        compoundRules = parseCompoundRules(reader, parseNum(reader, line));
      } else if ("COMPOUNDFLAG".equals(firstWord)) {
        compoundFlag = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDBEGIN".equals(firstWord)) {
        compoundBegin = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDMIDDLE".equals(firstWord)) {
        compoundMiddle = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDEND".equals(firstWord)) {
        compoundEnd = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDPERMITFLAG".equals(firstWord)) {
        compoundPermit = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDFORBIDFLAG".equals(firstWord)) {
        compoundForbid = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("CHECKCOMPOUNDCASE".equals(firstWord)) {
        checkCompoundCase = true;
      } else if ("CHECKCOMPOUNDDUP".equals(firstWord)) {
        checkCompoundDup = true;
      } else if ("CHECKCOMPOUNDREP".equals(firstWord)) {
        checkCompoundRep = true;
      } else if ("CHECKCOMPOUNDTRIPLE".equals(firstWord)) {
        checkCompoundTriple = true;
      } else if ("SIMPLIFIEDTRIPLE".equals(firstWord)) {
        simplifiedTriple = true;
      } else if ("CHECKCOMPOUNDPATTERN".equals(firstWord)) {
        int count = parseNum(reader, line);
        for (int i = 0; i < count; i++) {
          checkCompoundPatterns.add(
              new CheckCompoundPattern(reader.readLine(), flagParsingStrategy, this));
        }
      } else if ("SET".equals(firstWord)) {
        checkCriticalDirectiveSame(
            "SET", reader, decoder.charset(), getDecoder(singleArgument(reader, line)).charset());
      } else if ("FLAG".equals(firstWord)) {
        FlagParsingStrategy strategy = getFlagParsingStrategy(line, decoder.charset());
        checkCriticalDirectiveSame(
            "FLAG", reader, flagParsingStrategy.getClass(), strategy.getClass());
      }
    }

    this.prefixes = affixFST(prefixes);
    this.suffixes = affixFST(suffixes);
    secondStagePrefixFlags = toSortedCharArray(prefixContFlags);
    secondStageSuffixFlags = toSortedCharArray(suffixContFlags);

    int totalChars = 0;
    for (String strip : seenStrips.keySet()) {
      totalChars += strip.length();
    }
    stripData = new char[totalChars];
    stripOffsets = new int[seenStrips.size() + 1];
    int currentOffset = 0;
    int currentIndex = 0;
    for (String strip : seenStrips.keySet()) {
      stripOffsets[currentIndex++] = currentOffset;
      strip.getChars(0, strip.length(), stripData, currentOffset);
      currentOffset += strip.length();
    }
    assert currentIndex == seenStrips.size();
    stripOffsets[currentIndex] = currentOffset;
  }

  private void checkCriticalDirectiveSame(
      String directive, LineNumberReader reader, Object expected, Object actual)
      throws ParseException {
    if (!expected.equals(actual)) {
      throw new ParseException(
          directive
              + " directive should occur at most once, and in the first "
              + MAX_PROLOGUE_SCAN_WINDOW
              + " bytes of the *.aff file",
          reader.getLineNumber());
    }
  }

  private List<String> parseMapEntry(LineNumberReader reader, String line) throws ParseException {
    String unparsed = firstArgument(reader, line);
    List<String> mapEntry = new ArrayList<>();
    for (int j = 0; j < unparsed.length(); j++) {
      if (unparsed.charAt(j) == '(') {
        int closing = unparsed.indexOf(')', j);
        if (closing < 0) {
          throw new ParseException("Unclosed parenthesis: " + line, reader.getLineNumber());
        }

        mapEntry.add(unparsed.substring(j + 1, closing));
        j = closing;
      } else {
        mapEntry.add(String.valueOf(unparsed.charAt(j)));
      }
    }
    return mapEntry;
  }

  boolean hasLanguage(String... langCodes) {
    if (language == null) return false;
    String langCode = extractLanguageCode(language);
    for (String code : langCodes) {
      if (langCode.equals(code)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param root a string to look up in the dictionary. No case conversion or affix removal is
   *     performed. To get the possible roots of any word, you may call {@link
   *     Hunspell#getRoots(String)}
   * @return the dictionary entries for the given root, or {@code null} if there's none
   */
  public DictEntries lookupEntries(String root) {
    IntsRef forms = lookupWord(root.toCharArray(), 0, root.length());
    if (forms == null) return null;

    class DictEntriesImpl extends AbstractList<DictEntry> implements DictEntries {
      @Override
      public int size() {
        return forms.length / formStep();
      }

      @Override
      public DictEntry get(int entryIndex) {
        return dictEntry(
            root,
            forms.ints[forms.offset + (entryIndex * formStep())],
            hasCustomMorphData ? forms.ints[forms.offset + entryIndex * 2 + 1] : 0);
      }
    }
    return new DictEntriesImpl();
  }

  DictEntry dictEntry(String root, int flagId, int morphDataId) {
    return new DictEntry(root) {
      @Override
      public String getFlags() {
        return Dictionary.this.flagParsingStrategy.printFlags(flagLookup.getFlags(flagId));
      }

      @Override
      public String getMorphologicalData() {
        return morphDataId == 0 ? "" : morphData.get(morphDataId);
      }
    };
  }

  static String extractLanguageCode(String isoCode) {
    int underscore = isoCode.indexOf('_');
    return underscore < 0 ? isoCode : isoCode.substring(0, underscore);
  }

  private int parseNum(LineNumberReader reader, String line) throws ParseException {
    return Integer.parseInt(splitBySpace(reader, line, 2, Integer.MAX_VALUE)[1]);
  }

  private String singleArgument(LineNumberReader reader, String line) throws ParseException {
    return splitBySpace(reader, line, 2)[1];
  }

  private String firstArgument(LineNumberReader reader, String line) throws ParseException {
    return splitBySpace(reader, line, 2, Integer.MAX_VALUE)[1];
  }

  private String[] splitBySpace(LineNumberReader reader, String line, int expectedParts)
      throws ParseException {
    return splitBySpace(reader, line, expectedParts, expectedParts);
  }

  private String[] splitBySpace(LineNumberReader reader, String line, int minParts, int maxParts)
      throws ParseException {
    String[] parts = line.split("\\s+");
    if (parts.length < minParts || parts.length > maxParts && !parts[maxParts].startsWith("#")) {
      throw new ParseException("Invalid syntax: " + line, reader.getLineNumber());
    }
    return parts;
  }

  private CompoundRule[] parseCompoundRules(LineNumberReader reader, int num)
      throws IOException, ParseException {
    CompoundRule[] compoundRules = new CompoundRule[num];
    for (int i = 0; i < num; i++) {
      compoundRules[i] = new CompoundRule(singleArgument(reader, reader.readLine()), this);
    }
    return compoundRules;
  }

  private Breaks parseBreaks(LineNumberReader reader, String line)
      throws IOException, ParseException {
    Set<String> starting = new LinkedHashSet<>();
    Set<String> ending = new LinkedHashSet<>();
    Set<String> middle = new LinkedHashSet<>();
    int num = parseNum(reader, line);
    for (int i = 0; i < num; i++) {
      String breakStr = singleArgument(reader, reader.readLine());
      if (breakStr.startsWith("^")) {
        starting.add(breakStr.substring(1));
      } else if (breakStr.endsWith("$")) {
        ending.add(breakStr.substring(0, breakStr.length() - 1));
      } else {
        middle.add(breakStr);
      }
    }
    return new Breaks(starting, ending, middle);
  }

  private FST<IntsRef> affixFST(TreeMap<String, IntArrayList> affixes) throws IOException {
    IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
    FSTCompiler<IntsRef> fstCompiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, outputs).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (Map.Entry<String, IntArrayList> entry : affixes.entrySet()) {
      Util.toUTF32(entry.getKey(), scratch);
      IntArrayList entries = entry.getValue();
      IntsRef output = new IntsRef(entries.size());
      for (IntCursor c : entries) {
        output.ints[output.length++] = c.value;
      }
      fstCompiler.add(scratch.get(), output);
    }
    return FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());
  }

  /**
   * Parses a specific affix rule putting the result into the provided affix map
   *
   * @param affixes Map where the result of the parsing will be put
   * @param header Header line of the affix rule
   * @param reader BufferedReader to read the content of the rule from
   * @param seenPatterns map from condition -&gt; index of patterns, for deduplication.
   * @throws IOException Can be thrown while reading the rule
   */
  private void parseAffix(
      TreeMap<String, IntArrayList> affixes,
      CharHashSet secondStageFlags,
      String header,
      LineNumberReader reader,
      AffixKind kind,
      Map<String, Integer> seenPatterns,
      Map<String, Integer> seenStrips,
      FlagEnumerator flags)
      throws IOException, ParseException {

    StringBuilder sb = new StringBuilder();
    String[] args = header.split("\\s+");

    boolean crossProduct = args[2].equals("Y");

    int numLines;
    try {
      numLines = Integer.parseInt(args[3]);
    } catch (
        @SuppressWarnings("unused")
        NumberFormatException e) {
      if (tolerateAffixRuleCountMismatches()) {
        return;
      }
      throw new ParseException("Affix rule header expected; got " + header, reader.getLineNumber());
    }
    affixData = ArrayUtil.grow(affixData, currentAffix * 4 + numLines * 4);

    for (int i = 0; i < numLines; i++) {
      String line = reader.readLine();
      if (line == null) {
        throw new ParseException("Premature end of rules for " + header, reader.getLineNumber());
      }

      // from the manpage: PFX flag stripping prefix [condition [morphological_fields...]]
      String[] ruleArgs = splitBySpace(reader, line, 4, Integer.MAX_VALUE);

      if (!ruleArgs[1].equals(args[1])) {
        throw new ParseException(
            "Affix rule mismatch. Header: " + header + "; rule: " + line, reader.getLineNumber());
      }

      char flag = flagParsingStrategy.parseFlag(ruleArgs[1]);
      String strip = ruleArgs[2].equals("0") ? "" : ruleArgs[2];
      String affixArg = ruleArgs[3];
      char[] appendFlags = null;

      // first: parse continuation classes out of affix
      int flagSep = affixArg.lastIndexOf('/');
      if (flagSep != -1) {
        String flagPart = affixArg.substring(flagSep + 1);
        affixArg = affixArg.substring(0, flagSep);

        if (aliasCount > 0) {
          flagPart = getAliasValue(Integer.parseInt(flagPart));
        }

        appendFlags = flagParsingStrategy.parseFlags(flagPart);
        for (char appendFlag : appendFlags) {
          secondStageFlags.add(appendFlag);
        }
      }
      // zero affix -> empty string
      if ("0".equals(affixArg)) {
        affixArg = "";
      }

      String condition = ruleArgs.length > 4 ? ruleArgs[4] : ".";
      String key = AffixCondition.uniqueKey(kind, strip, condition);

      // deduplicate patterns
      Integer patternIndex = seenPatterns.get(key);
      if (patternIndex == null) {
        patternIndex = patterns.size();
        if (patternIndex > Short.MAX_VALUE) {
          throw new UnsupportedOperationException(
              "Too many patterns, please report this to dev@lucene.apache.org");
        }
        seenPatterns.put(key, patternIndex);
        patterns.add(AffixCondition.compile(kind, strip, condition, line));
      }

      Integer stripOrd = seenStrips.get(strip);
      if (stripOrd == null) {
        stripOrd = seenStrips.size();
        seenStrips.put(strip, stripOrd);
        if (stripOrd > Character.MAX_VALUE) {
          throw new UnsupportedOperationException(
              "Too many unique strips, please report this to dev@lucene.apache.org");
        }
      }

      if (appendFlags == null) {
        appendFlags = NOFLAGS;
      }

      int appendFlagsOrd = flags.add(appendFlags);
      if (appendFlagsOrd < 0) {
        // already exists in our hash
        appendFlagsOrd = (-appendFlagsOrd) - 1;
      } else if (appendFlagsOrd > Short.MAX_VALUE) {
        // this limit is probably flexible, but it's a good sanity check too
        throw new UnsupportedOperationException(
            "Too many unique append flags, please report this to dev@lucene.apache.org");
      }

      int dataStart = currentAffix * 4;
      affixData[dataStart + AFFIX_FLAG] = flag;
      affixData[dataStart + AFFIX_STRIP_ORD] = (char) stripOrd.intValue();
      // encode crossProduct into patternIndex
      int patternOrd = patternIndex << 1 | (crossProduct ? 1 : 0);
      affixData[dataStart + AFFIX_CONDITION] = (char) patternOrd;
      affixData[dataStart + AFFIX_APPEND] = (char) appendFlagsOrd;

      if (needsInputCleaning(affixArg)) {
        affixArg = cleanInput(affixArg, sb).toString();
      }

      if (kind == SUFFIX) {
        affixArg = new StringBuilder(affixArg).reverse().toString();
      }

      affixes.computeIfAbsent(affixArg, __ -> new IntArrayList()).add(currentAffix);
      currentAffix++;
    }
  }

  char affixData(int affixIndex, int offset) {
    return affixData[affixIndex * 4 + offset];
  }

  boolean isCrossProduct(int affix) {
    return (affixData(affix, AFFIX_CONDITION) & 1) == 1;
  }

  int getAffixCondition(int affix) {
    return affixData(affix, AFFIX_CONDITION) >>> 1;
  }

  private ConvTable parseConversions(LineNumberReader reader, int num)
      throws IOException, ParseException {
    TreeMap<String, String> mappings = new TreeMap<>();

    for (int i = 0; i < num; i++) {
      String[] parts = splitBySpace(reader, reader.readLine(), 3);
      if (mappings.put(parts[1], parts[2]) != null && !tolerateDuplicateConversionMappings()) {
        throw new IllegalStateException("duplicate mapping specified for: " + parts[1]);
      }
    }

    return new ConvTable(mappings);
  }

  private static final byte[] BOM_UTF8 = {(byte) 0xef, (byte) 0xbb, (byte) 0xbf};

  /** Parses the encoding and flag format specified in the provided InputStream */
  private void readConfig(InputStream stream, Charset streamCharset)
      throws IOException, ParseException {
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, streamCharset));
    String line;
    String flagLine = null;
    boolean charsetFound = false;
    boolean flagFound = false;
    while ((line = reader.readLine()) != null) {
      if (line.isBlank()) continue;

      String firstWord = line.split("\\s")[0];
      if ("SET".equals(firstWord)) {
        decoder = getDecoder(singleArgument(reader, line));
        charsetFound = true;
      } else if ("FLAG".equals(firstWord)) {
        // Preserve the flag line for parsing later since we need the decoder's charset
        // and just in case they come out of order.
        flagLine = line;
        flagFound = true;
      } else {
        continue;
      }

      if (charsetFound && flagFound) {
        break;
      }
    }

    if (flagFound) {
      flagParsingStrategy = getFlagParsingStrategy(flagLine, decoder.charset());
    }
  }

  /**
   * Consume the provided byte sequence in full, if present. Otherwise leave the input stream
   * intact.
   *
   * @return {@code true} if the sequence matched and has been consumed.
   */
  @SuppressWarnings("SameParameterValue")
  private static boolean maybeConsume(BufferedInputStream stream, byte[] bytes) throws IOException {
    stream.mark(bytes.length);
    for (byte b : bytes) {
      int nextByte = stream.read();
      if (nextByte != (b & 0xff)) { // covers EOF (-1) as well.
        stream.reset();
        return false;
      }
    }
    return true;
  }

  static final Map<String, String> CHARSET_ALIASES =
      Map.of("microsoft-cp1251", "windows-1251", "TIS620-2533", "TIS-620");

  /**
   * Retrieves the CharsetDecoder for the given encoding. Note, This isn't perfect as I think
   * ISCII-DEVANAGARI and MICROSOFT-CP1251 etc are allowed...
   *
   * @param encoding Encoding to retrieve the CharsetDecoder for
   * @return CharSetDecoder for the given encoding
   */
  private CharsetDecoder getDecoder(String encoding) {
    if ("ISO8859-14".equals(encoding)) {
      return new ISO8859_14Decoder();
    }
    String canon = CHARSET_ALIASES.get(encoding);
    if (canon != null) {
      encoding = canon;
    }
    return replacingDecoder(Charset.forName(encoding));
  }

  private static CharsetDecoder replacingDecoder(Charset charset) {
    return charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE);
  }

  /**
   * Determines the appropriate {@link FlagParsingStrategy} based on the FLAG definition line taken
   * from the affix file
   *
   * @param flagLine Line containing the flag information
   * @return FlagParsingStrategy that handles parsing flags in the way specified in the FLAG
   *     definition
   */
  static FlagParsingStrategy getFlagParsingStrategy(String flagLine, Charset charset) {
    String[] parts = flagLine.split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Illegal FLAG specification: " + flagLine);
    }
    String flagType = parts[1];

    if ("num".equals(flagType)) {
      return new NumFlagParsingStrategy();
    } else if ("UTF-8".equals(flagType)) {
      if (DEFAULT_CHARSET.equals(charset)) {
        return new DefaultAsUtf8FlagParsingStrategy();
      }
      return new SimpleFlagParsingStrategy();
    } else if ("long".equals(flagType)) {
      return new DoubleASCIIFlagParsingStrategy();
    }

    throw new IllegalArgumentException("Unknown flag type: " + flagType);
  }

  private static final char FLAG_SEPARATOR = 0x1f; // flag separator after escaping
  private static final char MORPH_SEPARATOR =
      0x1e; // separator for boundary of entry (may be followed by morph data)

  private String unescapeEntry(String entry) {
    StringBuilder sb = new StringBuilder();
    int end = morphBoundary(entry);
    for (int i = 0; i < end; i++) {
      char ch = entry.charAt(i);
      if (ch == '\\' && i + 1 < entry.length()) {
        sb.append(entry.charAt(i + 1));
        i++;
      } else if (ch == '/' && i > 0) {
        sb.append(FLAG_SEPARATOR);
      } else if (!shouldSkipEscapedChar(ch)) {
        sb.append(ch);
      }
    }
    sb.append(MORPH_SEPARATOR);
    if (end < entry.length()) {
      for (int i = end; i < entry.length(); i++) {
        char c = entry.charAt(i);
        if (!shouldSkipEscapedChar(c)) {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }

  private static boolean shouldSkipEscapedChar(char ch) {
    return ch == FLAG_SEPARATOR
        || ch == MORPH_SEPARATOR; // BINARY EXECUTABLES EMBEDDED IN ZULU DICTIONARIES!!!!!!!
  }

  private static int morphBoundary(String line) {
    int end = indexOfSpaceOrTab(line, 0);
    if (end == -1) {
      return line.length();
    }
    while (end >= 0 && end < line.length()) {
      if (line.charAt(end) == '\t'
          || end > 0
              && end + 3 < line.length()
              && Character.isLetter(line.charAt(end + 1))
              && Character.isLetter(line.charAt(end + 2))
              && line.charAt(end + 3) == ':') {
        break;
      }
      end = indexOfSpaceOrTab(line, end + 1);
    }
    if (end == -1) {
      return line.length();
    }
    return end;
  }

  static int indexOfSpaceOrTab(String text, int start) {
    int pos1 = text.indexOf('\t', start);
    int pos2 = text.indexOf(' ', start);
    if (pos1 >= 0 && pos2 >= 0) {
      return Math.min(pos1, pos2);
    } else {
      return Math.max(pos1, pos2);
    }
  }

  private void mergeDictionaries(
      List<InputStream> dictionaries, CharsetDecoder decoder, EntryAccumulator acc)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    for (InputStream dictionary : dictionaries) {
      BufferedReader lines = new BufferedReader(new InputStreamReader(dictionary, decoder));
      lines.readLine(); // first line is number of entries (approximately, sometimes)

      String line;
      while ((line = lines.readLine()) != null) {
        // wild and unpredictable code comment rules
        if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '\t') {
          continue;
        }
        line = unescapeEntry(line);
        // if we haven't seen any custom morphological data, try to parse one
        if (!hasCustomMorphData) {
          int morphStart = line.indexOf(MORPH_SEPARATOR);
          if (morphStart >= 0) {
            String data = line.substring(morphStart + 1);
            hasCustomMorphData = splitMorphData(data).stream().anyMatch(s -> !s.startsWith("ph:"));
          }
        }

        writeNormalizedWordEntry(sb, line, acc);
      }
    }
  }

  private void writeNormalizedWordEntry(StringBuilder reuse, String line, EntryAccumulator acc)
      throws IOException {
    int flagSep = line.indexOf(FLAG_SEPARATOR);
    int morphSep = line.indexOf(MORPH_SEPARATOR);
    assert morphSep > 0;
    assert morphSep > flagSep;
    int sep = flagSep < 0 ? morphSep : flagSep;
    if (sep == 0) return;

    CharSequence toWrite;
    String beforeSep = line.substring(0, sep);
    if (needsInputCleaning(beforeSep)) {
      cleanInput(beforeSep, reuse);
      reuse.append(line, sep, line.length());
      toWrite = reuse;
    } else {
      toWrite = line;
    }

    String written = toWrite.toString();
    sep = written.length() - (line.length() - sep);
    acc.addEntry(written);

    WordCase wordCase = WordCase.caseOf(written, sep);
    if (wordCase == WordCase.MIXED || wordCase == WordCase.UPPER && flagSep > 0) {
      addHiddenCapitalizedWord(reuse, acc, written.substring(0, sep), written.substring(sep));
    }
  }

  private void addHiddenCapitalizedWord(
      StringBuilder reuse, EntryAccumulator acc, String word, String afterSep) throws IOException {
    reuse.setLength(0);
    reuse.append(Character.toUpperCase(word.charAt(0)));
    for (int i = 1; i < word.length(); i++) {
      reuse.append(caseFold(word.charAt(i)));
    }
    reuse.append(FLAG_SEPARATOR);
    reuse.append(HIDDEN_FLAG);
    reuse.append(afterSep, afterSep.charAt(0) == FLAG_SEPARATOR ? 1 : 0, afterSep.length());
    acc.addEntry(reuse.toString());
  }

  String toLowerCase(String word) {
    char[] chars = new char[word.length()];
    for (int i = 0; i < word.length(); i++) {
      chars[i] = caseFold(word.charAt(i));
    }
    return new String(chars);
  }

  String toTitleCase(String word) {
    char[] chars = new char[word.length()];
    chars[0] = Character.toUpperCase(word.charAt(0));
    for (int i = 1; i < word.length(); i++) {
      chars[i] = caseFold(word.charAt(i));
    }
    return new String(chars);
  }

  private WordStorage readSortedDictionaries(FlagEnumerator flags, EntrySupplier sorted)
      throws IOException {
    Map<String, Integer> morphIndices = new HashMap<>();

    WordStorage.Builder builder =
        new WordStorage.Builder(
            sorted.wordCount(), hashFactor(), hasCustomMorphData, flags, allNonSuggestibleFlags());

    // TODO: the flags themselves can be double-chars (long) or also numeric
    // either way the trick is to encode them as char... but they must be parsed differently

    while (true) {
      String line = sorted.next();
      if (line == null) break;

      String entry;
      char[] wordForm;
      int end;

      int flagSep = line.indexOf(FLAG_SEPARATOR);
      if (flagSep == -1) {
        wordForm = NOFLAGS;
        end = line.indexOf(MORPH_SEPARATOR);
        entry = line.substring(0, end);
      } else {
        end = line.indexOf(MORPH_SEPARATOR);
        boolean hidden = line.charAt(flagSep + 1) == HIDDEN_FLAG;
        String flagPart = line.substring(flagSep + (hidden ? 2 : 1), end).strip();
        if (aliasCount > 0 && !flagPart.isEmpty()) {
          flagPart = getAliasValue(Integer.parseInt(flagPart));
        }

        wordForm = flagParsingStrategy.parseFlags(flagPart);
        if (hidden) {
          wordForm = ArrayUtil.growExact(wordForm, wordForm.length + 1);
          wordForm[wordForm.length - 1] = HIDDEN_FLAG;
        }
        entry = line.substring(0, flagSep);
      }

      if (entry.isEmpty()) continue;

      int morphDataID = 0;
      if (end + 1 < line.length()) {
        List<String> morphFields = readMorphFields(entry, line.substring(end + 1));
        if (!morphFields.isEmpty()) {
          morphFields.sort(Comparator.naturalOrder());
          morphDataID = addMorphFields(morphIndices, String.join(" ", morphFields));
        }
      }

      builder.add(entry, wordForm, morphDataID);
    }

    return builder.build();
  }

  /**
   * The factor determining the size of the internal hash table used for storing the entries. The
   * table size is {@code entry_count * hashFactor}. The default factor is 1.0. If there are too
   * many hash collisions, the factor can be increased, resulting in faster access, but more memory
   * usage.
   */
  protected double hashFactor() {
    return 1.0;
  }

  /**
   * Whether incorrect PFX/SFX rule counts should be silently ignored. False by default: a {@link
   * ParseException} will happen.
   */
  protected boolean tolerateAffixRuleCountMismatches() {
    return false;
  }

  /**
   * Whether duplicate ICONV/OCONV lines should be silently ignored. False by default: an {@link
   * IllegalStateException} will happen.
   */
  protected boolean tolerateDuplicateConversionMappings() {
    return false;
  }

  char[] allNonSuggestibleFlags() {
    CharHashSet set = new CharHashSet(5);
    set.add(HIDDEN_FLAG);
    for (char c : new char[] {noSuggest, forbiddenword, onlyincompound, subStandard}) {
      if (c != FLAG_UNSET) {
        set.add(c);
      }
    }
    return Dictionary.toSortedCharArray(set);
  }

  private List<String> readMorphFields(String word, String unparsed) {
    List<String> morphFields = null;
    for (String datum : splitMorphData(unparsed)) {
      if (datum.startsWith("ph:")) {
        addPhoneticRepEntries(word, datum.substring(3));
      } else {
        if (morphFields == null) morphFields = new ArrayList<>(1);
        morphFields.add(datum);
      }
    }
    return morphFields == null ? Collections.emptyList() : morphFields;
  }

  private int addMorphFields(Map<String, Integer> indices, String morphFields) {
    Integer alreadyCached = indices.get(morphFields);
    if (alreadyCached != null) {
      return alreadyCached;
    }

    int index = morphData.size();
    indices.put(morphFields, index);
    morphData.add(morphFields);
    return index;
  }

  private void addPhoneticRepEntries(String word, String ph) {
    // e.g. "pretty ph:prity ph:priti->pretti" to suggest both prity->pretty and pritier->prettiest
    int arrow = ph.indexOf("->");
    String pattern;
    String replacement;
    if (arrow > 0) {
      pattern = ph.substring(0, arrow);
      replacement = ph.substring(arrow + 2);
    } else {
      pattern = ph;
      replacement = word;
    }

    // when the ph: field ends with *, strip last character of pattern and replacement
    // e.g., "pretty ph:prity*" results in "prit->prett" replacement instead of "prity->pretty",
    // to get both prity->pretty and pritiest->prettiest suggestions.
    if (pattern.endsWith("*") && pattern.length() > 2 && replacement.length() > 1) {
      pattern = pattern.substring(0, pattern.length() - 2);
      replacement = replacement.substring(0, replacement.length() - 1);
    }

    // capitalize lowercase pattern for capitalized words to support
    // good suggestions also for capitalized misspellings,
    // e.g. Wednesday ph:wendsay results in wendsay -> Wednesday and Wendsay -> Wednesday.
    if (WordCase.caseOf(word) == WordCase.TITLE && WordCase.caseOf(pattern) == WordCase.LOWER) {
      // add also lowercase word in the case of German or
      // Hungarian to support lowercase suggestions lowercased by
      // compound word generation or derivational suffixes
      // for example by adjectival suffix "-i" of geographical names in Hungarian:
      // Massachusetts ph:messzecsuzec
      // messzecsuzeci -> massachusettsi (adjective)
      // For lowercasing by conditional PFX rules, see e.g. germancompounding test
      if (hasLanguage("de", "hu")) {
        repTable.add(new RepEntry(pattern, toLowerCase(replacement)));
      }
      repTable.add(new RepEntry(toTitleCase(pattern), replacement));
    }
    repTable.add(new RepEntry(pattern, replacement));
  }

  boolean isDotICaseChangeDisallowed(char[] word) {
    return word[0] == 'İ' && !alternateCasing;
  }

  private void parseAlias(String line) {
    String[] ruleArgs = line.split("\\s+");
    if (aliases == null) {
      // first line should be the aliases count
      final int count = Integer.parseInt(ruleArgs[1]);
      aliases = new String[count];
    } else {
      // an alias can map to no flags
      String aliasValue = ruleArgs.length == 1 ? "" : ruleArgs[1];
      aliases[aliasCount++] = aliasValue;
    }
  }

  private String getAliasValue(int id) {
    try {
      return aliases[id - 1];
    } catch (IndexOutOfBoundsException ex) {
      throw new IllegalArgumentException("Bad flag alias number:" + id, ex);
    }
  }

  private void parseMorphAlias(String line) {
    if (morphAliases == null) {
      // first line should be the aliases count
      final int count = Integer.parseInt(line.substring(3));
      morphAliases = new String[count];
    } else {
      String arg = line.substring(2); // leave the space
      morphAliases[morphAliasCount++] = arg;
    }
  }

  private List<String> splitMorphData(String morphData) {
    // first see if it's an alias
    if (morphAliasCount > 0) {
      try {
        int alias = Integer.parseInt(morphData.trim());
        morphData = morphAliases[alias - 1];
      } catch (
          @SuppressWarnings("unused")
          NumberFormatException ignored) {
      }
    }
    if (morphData.isBlank()) {
      return Collections.emptyList();
    }

    List<String> result = null;
    int start = 0;
    for (int i = 0; i <= morphData.length(); i++) {
      if (i == morphData.length() || Character.isWhitespace(morphData.charAt(i))) {
        if (i - start > 3
            && Character.isLetter(morphData.charAt(start))
            && Character.isLetter(morphData.charAt(start + 1))
            && morphData.charAt(start + 2) == ':') {
          if (result == null) result = new ArrayList<>();
          result.add(morphData.substring(start, i));
        }
        start = i + 1;
      }
    }
    return result == null ? List.of() : result;
  }

  boolean hasFlag(IntsRef forms, char flag) {
    int formStep = formStep();
    for (int i = 0; i < forms.length; i += formStep) {
      if (hasFlag(forms.ints[forms.offset + i], flag)) {
        return true;
      }
    }
    return false;
  }

  boolean isFlagAppendedByAffix(int affixId, char flag) {
    if (affixId < 0 || flag == FLAG_UNSET) return false;
    int appendId = affixData(affixId, AFFIX_APPEND);
    return hasFlag(appendId, flag);
  }

  /** Abstraction of the process of parsing flags taken from the affix and dic files */
  abstract static class FlagParsingStrategy {
    // we don't check the flag count, as Hunspell accepts longer sequences
    // https://github.com/hunspell/hunspell/issues/707
    static final boolean checkFlags = false;

    /**
     * Parses the given String into a single flag
     *
     * @param rawFlag String to parse into a flag
     * @return Parsed flag
     */
    char parseFlag(String rawFlag) {
      char[] flags = parseFlags(rawFlag);
      if (checkFlags && flags.length != 1) {
        throw new IllegalArgumentException("expected only one flag, got: " + rawFlag);
      }
      return flags[0];
    }

    /**
     * Parses the given String into multiple flags
     *
     * @param rawFlags String to parse into flags
     * @return Parsed flags
     */
    abstract char[] parseFlags(String rawFlags);

    /**
     * @return the original string representation of the given flag encoded by {@link #parseFlags}.
     */
    abstract String printFlag(char flag);

    /**
     * @return a presentable sorted concatenation of {@link #printFlag} results
     */
    String printFlags(char[] encodedFlags) {
      List<String> printed = new ArrayList<>();
      for (char c : encodedFlags) {
        if (c >= DEFAULT_FLAGS) continue;
        printed.add(printFlag(c));
      }
      String delimiter = this instanceof NumFlagParsingStrategy ? "," : "";
      return printed.stream().sorted().collect(Collectors.joining(delimiter));
    }

    /** Parse flags from a string resulting from {@link #printFlags} */
    char[] parseUtfFlags(String flagsInUtf) {
      return parseFlags(flagsInUtf);
    }
  }

  /**
   * Simple implementation of {@link FlagParsingStrategy} that treats the chars in each String as a
   * individual flags. Can be used with both the ASCII and UTF-8 flag types.
   */
  private static class SimpleFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      return rawFlags.toCharArray();
    }

    @Override
    String printFlag(char flag) {
      return String.valueOf(flag);
    }
  }

  /** Used to read flags as UTF-8 even if the rest of the file is in the default (8-bit) encoding */
  private static class DefaultAsUtf8FlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      return new String(rawFlags.getBytes(DEFAULT_CHARSET), StandardCharsets.UTF_8).toCharArray();
    }

    @Override
    String printFlag(char flag) {
      return String.valueOf(flag);
    }

    @Override
    char[] parseUtfFlags(String flagsInUtf) {
      return flagsInUtf.toCharArray();
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded in its
   * numerical form. In the case of multiple flags, each number is separated by a comma.
   */
  private static class NumFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      StringBuilder result = new StringBuilder();
      StringBuilder group = new StringBuilder();
      for (int i = 0; i <= rawFlags.length(); i++) {
        if (i == rawFlags.length() || rawFlags.charAt(i) == ',') {
          if (group.length() > 0) { // ignoring empty flags (this happens in danish, for example)
            int flag = Integer.parseInt(group, 0, group.length(), 10);
            if (flag >= DEFAULT_FLAGS) {
              // accept 0 due to https://github.com/hunspell/hunspell/issues/708
              throw new IllegalArgumentException(
                  "Num flags should be between 0 and " + DEFAULT_FLAGS + ", found " + flag);
            }
            result.append((char) flag);
            group.setLength(0);
          }
        } else if (rawFlags.charAt(i) >= '0' && rawFlags.charAt(i) <= '9') {
          group.append(rawFlags.charAt(i));
        }
      }

      return result.toString().toCharArray();
    }

    @Override
    String printFlag(char flag) {
      return String.valueOf((int) flag);
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded as two ASCII
   * characters whose codes must be combined into a single character.
   */
  private static class DoubleASCIIFlagParsingStrategy extends FlagParsingStrategy {

    @Override
    public char[] parseFlags(String rawFlags) {
      if (checkFlags && rawFlags.length() % 2 == 1) {
        throw new IllegalArgumentException(
            "Invalid flags (should be even number of characters): " + rawFlags);
      }

      char[] flags = new char[rawFlags.length() / 2];
      for (int i = 0; i < flags.length; i++) {
        char f1 = rawFlags.charAt(i * 2);
        char f2 = rawFlags.charAt(i * 2 + 1);
        if (f1 >= 256 || f2 >= 256) {
          throw new IllegalArgumentException(
              "Invalid flags (LONG flags must be double ASCII): " + rawFlags);
        }
        flags[i] = (char) (f1 << 8 | f2);
      }
      return flags;
    }

    @Override
    String printFlag(char flag) {
      return new String(new char[] {(char) ((flag & 0xff00) >>> 8), (char) (flag & 0xff)});
    }
  }

  boolean hasFlag(int entryId, char flag) {
    return flagLookup.hasFlag(entryId, flag);
  }

  boolean mayNeedInputCleaning() {
    return ignoreCase || ignore != null || iconv != null;
  }

  boolean needsInputCleaning(CharSequence input) {
    if (mayNeedInputCleaning()) {
      for (int i = 0; i < input.length(); i++) {
        char ch = input.charAt(i);
        if (ignore != null && Arrays.binarySearch(ignore, ch) >= 0
            || ignoreCase && caseFold(ch) != ch
            || iconv != null && iconv.mightReplaceChar(ch)) {
          return true;
        }
      }
    }
    return false;
  }

  CharSequence cleanInput(CharSequence input, StringBuilder reuse) {
    reuse.setLength(0);

    for (int i = 0; i < input.length(); i++) {
      char ch = input.charAt(i);

      if (ignore != null && Arrays.binarySearch(ignore, ch) >= 0) {
        continue;
      }

      if (ignoreCase && iconv == null) {
        // if we have no input conversion mappings, do this on-the-fly
        ch = caseFold(ch);
      }

      reuse.append(ch);
    }

    if (iconv != null) {
      iconv.applyMappings(reuse);
      if (ignoreCase) {
        for (int i = 0; i < reuse.length(); i++) {
          reuse.setCharAt(i, caseFold(reuse.charAt(i)));
        }
      }
    }

    return reuse;
  }

  static char[] toSortedCharArray(CharHashSet set) {
    char[] chars = set.toArray();
    Arrays.sort(chars);
    return chars;
  }

  boolean isSecondStagePrefix(char flag) {
    return Arrays.binarySearch(secondStagePrefixFlags, flag) >= 0;
  }

  boolean isSecondStageSuffix(char flag) {
    return Arrays.binarySearch(secondStageSuffixFlags, flag) >= 0;
  }

  /** folds single character (according to LANG if present) */
  char caseFold(char c) {
    if (alternateCasing) {
      if (c == 'I') {
        return 'ı';
      } else if (c == 'İ') {
        return 'i';
      } else {
        return Character.toLowerCase(c);
      }
    } else {
      return Character.toLowerCase(c);
    }
  }

  /** Returns true if this dictionary was constructed with the {@code ignoreCase} option */
  public boolean getIgnoreCase() {
    return ignoreCase;
  }

  /**
   * Returns the default temporary directory pointed to by {@code java.io.tmpdir}. If not accessible
   * or not available, an IOException is thrown.
   */
  static Path getDefaultTempDir() throws IOException {
    String tmpDir = System.getProperty("java.io.tmpdir");
    if (tmpDir == null) {
      throw new IOException("No temporary path (java.io.tmpdir)?");
    }
    Path tmpPath = Paths.get(tmpDir);
    if (!Files.isWritable(tmpPath)) {
      throw new IOException(
          "Temporary path not present or writeable?: " + tmpPath.toAbsolutePath());
    }
    return tmpPath;
  }

  /** Possible word breaks according to BREAK directives */
  static class Breaks {
    private static final Set<String> MINUS = Collections.singleton("-");
    static final Breaks DEFAULT = new Breaks(MINUS, MINUS, MINUS);
    final String[] starting, ending, middle;

    Breaks(Collection<String> starting, Collection<String> ending, Collection<String> middle) {
      this.starting = starting.toArray(new String[0]);
      this.ending = ending.toArray(new String[0]);
      this.middle = middle.toArray(new String[0]);
    }

    boolean isNotEmpty() {
      return middle.length > 0 || starting.length > 0 || ending.length > 0;
    }
  }
}
