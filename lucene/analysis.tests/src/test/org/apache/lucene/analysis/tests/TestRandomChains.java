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
package org.apache.lucene.analysis.tests;

import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.Transliterator;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.Caverphone2;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.Nysiis;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizerConfig;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenPositionFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.apache.lucene.analysis.pattern.PatternTypingFilter;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.stempel.StempelStemmer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.word2vec.Word2VecModel;
import org.apache.lucene.analysis.synonym.word2vec.Word2VecSynonymProvider;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.analysis.ValidatingTokenFilter;
import org.apache.lucene.tests.util.Rethrow;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IgnoreRandomChains;
import org.apache.lucene.util.TermAndVector;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.tartarus.snowball.SnowballStemmer;
import org.xml.sax.InputSource;

/** tests random analysis chains */
public class TestRandomChains extends BaseTokenStreamTestCase {

  static List<Constructor<? extends Tokenizer>> tokenizers;
  static List<Constructor<? extends TokenFilter>> tokenfilters;
  static List<Constructor<? extends CharFilter>> charfilters;

  static List<Class<? extends SnowballStemmer>> snowballStemmers;

  private static final Set<Class<?>> avoidConditionals =
      Set.of(
          FingerprintFilter.class,
          MinHashFilter.class,
          ConcatenateGraphFilter.class,
          // ShingleFilter doesn't handle input graphs correctly, so wrapping it in a condition can
          // expose inconsistent offsets
          // https://issues.apache.org/jira/browse/LUCENE-4170
          ShingleFilter.class,
          FixedShingleFilter.class,
          // FlattenGraphFilter changes the output graph entirely, so wrapping it in a condition
          // can break position lengths
          FlattenGraphFilter.class,
          // LimitToken*Filters don't set end offsets correctly
          LimitTokenOffsetFilter.class,
          LimitTokenCountFilter.class,
          LimitTokenPositionFilter.class);

  private static final Map<Constructor<?>, Predicate<Object[]>> brokenConstructors;

  static {
    try {
      final Map<Constructor<?>, Predicate<Object[]>> map = new HashMap<>();
      // LimitToken*Filter can only use special ctor when last arg is true
      for (final var c :
          List.of(
              LimitTokenCountFilter.class,
              LimitTokenOffsetFilter.class,
              LimitTokenPositionFilter.class)) {
        map.put(
            c.getConstructor(TokenStream.class, int.class, boolean.class),
            args -> {
              assert args.length == 3;
              return false == ((Boolean) args[2]); // args are broken if consumeAllTokens is false
            });
      }
      brokenConstructors = Collections.unmodifiableMap(map);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private static final Map<Class<?>, Function<Random, Object>> argProducers =
      Collections.unmodifiableMap(
          new IdentityHashMap<Class<?>, Function<Random, Object>>() {
            {
              put(
                  int.class,
                  random -> {
                    // TODO: could cause huge ram usage to use full int range for some filters
                    // (e.g. allocate enormous arrays)
                    // return Integer.valueOf(random.nextInt());
                    return Integer.valueOf(TestUtil.nextInt(random, -50, 50));
                  });
              put(
                  char.class,
                  random -> {
                    // TODO: fix any filters that care to throw IAE instead.
                    // also add a unicode validating filter to validate termAtt?
                    // return Character.valueOf((char)random.nextInt(65536));
                    while (true) {
                      char c = (char) random.nextInt(65536);
                      if (c < '\uD800' || c > '\uDFFF') {
                        return Character.valueOf(c);
                      }
                    }
                  });
              put(float.class, Random::nextFloat);
              put(boolean.class, Random::nextBoolean);
              put(byte.class, random -> (byte) random.nextInt(256));
              put(
                  byte[].class,
                  random -> {
                    byte[] bytes = new byte[random.nextInt(256)];
                    random.nextBytes(bytes);
                    return bytes;
                  });
              put(Random.class, random -> new Random(random.nextLong()));
              put(Version.class, random -> Version.LATEST);
              put(AttributeFactory.class, BaseTokenStreamTestCase::newAttributeFactory);
              put(AttributeSource.class, random -> null); // force IAE/NPE
              put(
                  Set.class,
                  random -> {
                    // TypeTokenFilter
                    Set<String> set = new HashSet<>();
                    int num = random.nextInt(5);
                    for (int i = 0; i < num; i++) {
                      set.add(
                          StandardTokenizer.TOKEN_TYPES[
                              random.nextInt(StandardTokenizer.TOKEN_TYPES.length)]);
                    }
                    return set;
                  });
              put(
                  Collection.class,
                  random -> {
                    // CapitalizationFilter
                    Collection<char[]> col = new ArrayList<>();
                    int num = random.nextInt(5);
                    for (int i = 0; i < num; i++) {
                      col.add(TestUtil.randomSimpleString(random).toCharArray());
                    }
                    return col;
                  });
              put(
                  CharArraySet.class,
                  random -> {
                    int num = random.nextInt(10);
                    CharArraySet set = new CharArraySet(num, random.nextBoolean());
                    for (int i = 0; i < num; i++) {
                      // TODO: make nastier
                      set.add(TestUtil.randomSimpleString(random));
                    }
                    return set;
                  });
              // TODO: don't want to make the exponentially slow ones Dawid documents
              // in TestPatternReplaceFilter, so dont use truly random patterns (for now)
              put(Pattern.class, random -> Pattern.compile("a"));
              put(
                  Pattern[].class,
                  random ->
                      new Pattern[] {Pattern.compile("([a-z]+)"), Pattern.compile("([0-9]+)")});
              put(
                  PayloadEncoder.class,
                  random ->
                      new IdentityEncoder()); // the other encoders will throw exceptions if tokens
              // arent numbers?
              put(
                  Dictionary.class,
                  random -> {
                    // TODO: make nastier
                    InputStream affixStream =
                        TestRandomChains.class.getResourceAsStream("simple.aff");
                    InputStream dictStream =
                        TestRandomChains.class.getResourceAsStream("simple.dic");
                    try {
                      return new Dictionary(
                          new ByteBuffersDirectory(), "dictionary", affixStream, dictStream);
                    } catch (Exception ex) {
                      Rethrow.rethrow(ex);
                      return null; // unreachable code
                    }
                  });
              put(
                  HyphenationTree.class,
                  random -> {
                    // TODO: make nastier
                    try {
                      InputSource is =
                          new InputSource(
                              TestRandomChains.class.getResource("da_UTF8.xml").toExternalForm());
                      HyphenationTree hyphenator =
                          HyphenationCompoundWordTokenFilter.getHyphenationTree(is);
                      return hyphenator;
                    } catch (Exception ex) {
                      Rethrow.rethrow(ex);
                      return null; // unreachable code
                    }
                  });
              put(
                  SnowballStemmer.class,
                  random -> {
                    try {
                      var clazz = snowballStemmers.get(random.nextInt(snowballStemmers.size()));
                      return clazz.getConstructor().newInstance();
                    } catch (Exception ex) {
                      Rethrow.rethrow(ex);
                      return null; // unreachable code
                    }
                  });
              put(
                  String.class,
                  random -> {
                    // TODO: make nastier
                    if (random.nextBoolean()) {
                      // a token type
                      return StandardTokenizer.TOKEN_TYPES[
                          random.nextInt(StandardTokenizer.TOKEN_TYPES.length)];
                    } else {
                      return TestUtil.randomSimpleString(random);
                    }
                  });
              put(
                  NormalizeCharMap.class,
                  random -> {
                    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
                    // we can't add duplicate keys, or NormalizeCharMap gets angry
                    Set<String> keys = new HashSet<>();
                    int num = random.nextInt(5);
                    // System.out.println("NormalizeCharMap=");
                    for (int i = 0; i < num; i++) {
                      String key = TestUtil.randomSimpleString(random);
                      if (!keys.contains(key) && key.length() > 0) {
                        String value = TestUtil.randomSimpleString(random);
                        builder.add(key, value);
                        keys.add(key);
                        // System.out.println("mapping: '" + key + "' => '" + value + "'");
                      }
                    }
                    return builder.build();
                  });
              put(
                  CharacterRunAutomaton.class,
                  random -> {
                    // TODO: could probably use a purely random automaton
                    switch (random.nextInt(5)) {
                      case 0:
                        return MockTokenizer.KEYWORD;
                      case 1:
                        return MockTokenizer.SIMPLE;
                      case 2:
                        return MockTokenizer.WHITESPACE;
                      case 3:
                        return MockTokenFilter.EMPTY_STOPSET;
                      default:
                        return MockTokenFilter.ENGLISH_STOPSET;
                    }
                  });
              put(
                  CharArrayMap.class,
                  random -> {
                    int num = random.nextInt(10);
                    CharArrayMap<String> map = new CharArrayMap<>(num, random.nextBoolean());
                    for (int i = 0; i < num; i++) {
                      // TODO: make nastier
                      map.put(
                          TestUtil.randomSimpleString(random), TestUtil.randomSimpleString(random));
                    }
                    return map;
                  });
              put(
                  StemmerOverrideMap.class,
                  random -> {
                    int num = random.nextInt(10);
                    StemmerOverrideFilter.Builder builder =
                        new StemmerOverrideFilter.Builder(random.nextBoolean());
                    for (int i = 0; i < num; i++) {
                      String input = "";
                      do {
                        input = TestUtil.randomRealisticUnicodeString(random);
                      } while (input.isEmpty());
                      String out = "";
                      TestUtil.randomSimpleString(random);
                      do {
                        out = TestUtil.randomRealisticUnicodeString(random);
                      } while (out.isEmpty());
                      builder.add(input, out);
                    }
                    try {
                      return builder.build();
                    } catch (Exception ex) {
                      Rethrow.rethrow(ex);
                      return null; // unreachable code
                    }
                  });
              put(
                  SynonymMap.class,
                  new Function<Random, Object>() {
                    @Override
                    public Object apply(Random random) {
                      SynonymMap.Builder b = new SynonymMap.Builder(random.nextBoolean());
                      final int numEntries = atLeast(10);
                      for (int j = 0; j < numEntries; j++) {
                        addSyn(
                            b,
                            randomNonEmptyString(random),
                            randomNonEmptyString(random),
                            random.nextBoolean());
                      }
                      try {
                        return b.build();
                      } catch (Exception ex) {
                        Rethrow.rethrow(ex);
                        return null; // unreachable code
                      }
                    }

                    private void addSyn(
                        SynonymMap.Builder b, String input, String output, boolean keepOrig) {
                      b.add(
                          new CharsRef(input.replaceAll(" +", "\u0000")),
                          new CharsRef(output.replaceAll(" +", "\u0000")),
                          keepOrig);
                    }

                    private String randomNonEmptyString(Random random) {
                      while (true) {
                        final String s = TestUtil.randomUnicodeString(random).trim();
                        if (s.length() != 0 && s.indexOf('\u0000') == -1) {
                          return s;
                        }
                      }
                    }
                  });
              put(
                  Word2VecSynonymProvider.class,
                  random -> {
                    final int numEntries = atLeast(10);
                    final int vectorDimension = random.nextInt(99) + 1;
                    Word2VecModel model = new Word2VecModel(numEntries, vectorDimension);
                    for (int j = 0; j < numEntries; j++) {
                      String s = TestUtil.randomSimpleString(random, 10, 20);
                      float[] vec = new float[vectorDimension];
                      for (int i = 0; i < vectorDimension; i++) {
                        vec[i] = random.nextFloat();
                      }
                      model.addTermAndVector(new TermAndVector(new BytesRef(s), vec));
                    }
                    try {
                      return new Word2VecSynonymProvider(model);
                    } catch (IOException e) {
                      Rethrow.rethrow(e);
                      return null; // unreachable code
                    }
                  });
              put(
                  DateFormat.class,
                  random -> {
                    if (random.nextBoolean()) return null;
                    return DateFormat.getDateInstance(DateFormat.DEFAULT, randomLocale(random));
                  });
              put(
                  Automaton.class,
                  random -> {
                    return Operations.determinize(
                        new RegExp(AutomatonTestUtil.randomRegexp(random), RegExp.NONE)
                            .toAutomaton(),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
                  });
              put(
                  PatternTypingFilter.PatternTypingRule[].class,
                  random -> {
                    int numRules = TestUtil.nextInt(random, 1, 3);
                    PatternTypingFilter.PatternTypingRule[] patternTypingRules =
                        new PatternTypingFilter.PatternTypingRule[numRules];
                    for (int i = 0; i < patternTypingRules.length; i++) {
                      String s = TestUtil.randomSimpleString(random, 1, 2);
                      // random regex with one group
                      String regex = s + "(.*)";
                      // pattern rule with a template that accepts one group.
                      patternTypingRules[i] =
                          new PatternTypingFilter.PatternTypingRule(
                              Pattern.compile(regex), TestUtil.nextInt(random, 1, 8), s + "_$1");
                    }
                    return patternTypingRules;
                  });

              // ICU:
              put(
                  Normalizer2.class,
                  random -> {
                    switch (random.nextInt(5)) {
                      case 0:
                        return Normalizer2.getNFCInstance();
                      case 1:
                        return Normalizer2.getNFDInstance();
                      case 2:
                        return Normalizer2.getNFKCInstance();
                      case 3:
                        return Normalizer2.getNFKDInstance();
                      default:
                        return Normalizer2.getNFKCCasefoldInstance();
                    }
                  });
              final var icuTransliterators = Collections.list(Transliterator.getAvailableIDs());
              Collections.sort(icuTransliterators);
              put(
                  Transliterator.class,
                  random ->
                      Transliterator.getInstance(
                          icuTransliterators.get(random.nextInt(icuTransliterators.size()))));
              put(
                  ICUTokenizerConfig.class,
                  random ->
                      new DefaultICUTokenizerConfig(random.nextBoolean(), random.nextBoolean()));

              // Kuromoji:
              final var jaComplFilterModes = JapaneseCompletionFilter.Mode.values();
              put(
                  JapaneseCompletionFilter.Mode.class,
                  random -> jaComplFilterModes[random.nextInt(jaComplFilterModes.length)]);
              final var jaTokModes = JapaneseTokenizer.Mode.values();
              put(
                  JapaneseTokenizer.Mode.class,
                  random -> jaTokModes[random.nextInt(jaTokModes.length)]);
              put(org.apache.lucene.analysis.ja.dict.UserDictionary.class, random -> null);

              // Nori:
              final var koComplFilterModes = KoreanTokenizer.DecompoundMode.values();
              put(
                  KoreanTokenizer.DecompoundMode.class,
                  random -> koComplFilterModes[random.nextInt(koComplFilterModes.length)]);
              put(org.apache.lucene.analysis.ko.dict.UserDictionary.class, random -> null);

              // Phonetic:
              final var bmNameTypes = org.apache.commons.codec.language.bm.NameType.values();
              final var bmRuleTypes =
                  Stream.of(org.apache.commons.codec.language.bm.RuleType.values())
                      .filter(e -> e != org.apache.commons.codec.language.bm.RuleType.RULES)
                      .toArray(org.apache.commons.codec.language.bm.RuleType[]::new);
              put(
                  PhoneticEngine.class,
                  random ->
                      new PhoneticEngine(
                          bmNameTypes[random.nextInt(bmNameTypes.length)],
                          bmRuleTypes[random.nextInt(bmRuleTypes.length)],
                          random.nextBoolean()));
              put(
                  Encoder.class,
                  random -> {
                    switch (random.nextInt(7)) {
                      case 0:
                        return new DoubleMetaphone();
                      case 1:
                        return new Metaphone();
                      case 2:
                        return new Soundex();
                      case 3:
                        return new RefinedSoundex();
                      case 4:
                        return new Caverphone2();
                      case 5:
                        return new ColognePhonetic();
                      default:
                        return new Nysiis();
                    }
                  });

              // Stempel
              put(
                  StempelStemmer.class,
                  random -> new StempelStemmer(PolishAnalyzer.getDefaultTable()));
            }
          });

  static final Set<Class<?>> allowedTokenizerArgs = argProducers.keySet(),
      allowedTokenFilterArgs =
          union(argProducers.keySet(), List.of(TokenStream.class, CommonGramsFilter.class)),
      allowedCharFilterArgs = union(argProducers.keySet(), List.of(Reader.class));

  @BeforeClass
  public static void beforeClass() throws Exception {
    List<Class<?>> analysisClasses =
        ModuleClassDiscovery.getClassesForPackage("org.apache.lucene.analysis");
    tokenizers = new ArrayList<>();
    tokenfilters = new ArrayList<>();
    charfilters = new ArrayList<>();
    for (final Class<?> c : analysisClasses) {
      final int modifiers = c.getModifiers();
      if (
      // don't waste time with abstract classes, deprecated, or @IgnoreRandomChains annotated
      // classes:
      Modifier.isAbstract(modifiers)
          || !Modifier.isPublic(modifiers)
          || c.isSynthetic()
          || c.isAnonymousClass()
          || c.isMemberClass()
          || c.isInterface()
          || c.isAnnotationPresent(Deprecated.class)
          || c.isAnnotationPresent(IgnoreRandomChains.class)
          || !(Tokenizer.class.isAssignableFrom(c)
              || TokenFilter.class.isAssignableFrom(c)
              || CharFilter.class.isAssignableFrom(c))) {
        continue;
      }

      for (final Constructor<?> ctor : c.getConstructors()) {
        // don't test synthetic, deprecated, or @IgnoreRandomChains annotated ctors, they likely
        // have known bugs:
        if (ctor.isSynthetic()
            || ctor.isAnnotationPresent(Deprecated.class)
            || ctor.isAnnotationPresent(IgnoreRandomChains.class)) {
          continue;
        }
        // conditional filters are tested elsewhere
        if (ConditionalTokenFilter.class.isAssignableFrom(c)) {
          continue;
        }
        if (Tokenizer.class.isAssignableFrom(c)) {
          assertTrue(
              ctor.toGenericString() + " has unsupported parameter types",
              allowedTokenizerArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          tokenizers.add(castConstructor(Tokenizer.class, ctor));
        } else if (TokenFilter.class.isAssignableFrom(c)) {
          assertTrue(
              ctor.toGenericString() + " has unsupported parameter types",
              allowedTokenFilterArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          tokenfilters.add(castConstructor(TokenFilter.class, ctor));
        } else if (CharFilter.class.isAssignableFrom(c)) {
          assertTrue(
              ctor.toGenericString() + " has unsupported parameter types",
              allowedCharFilterArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          charfilters.add(castConstructor(CharFilter.class, ctor));
        } else {
          fail("Cannot get here");
        }
      }
    }

    final Comparator<Constructor<?>> ctorComp = Comparator.comparing(Constructor::toGenericString);
    Collections.sort(tokenizers, ctorComp);
    Collections.sort(tokenfilters, ctorComp);
    Collections.sort(charfilters, ctorComp);
    if (VERBOSE) {
      System.out.println("tokenizers = " + tokenizers);
      System.out.println("tokenfilters = " + tokenfilters);
      System.out.println("charfilters = " + charfilters);
    }

    // TODO: Eclipse does not get that cast right, so make explicit:
    final Function<Class<?>, Class<? extends SnowballStemmer>> stemmerCast =
        c -> c.asSubclass(SnowballStemmer.class);
    snowballStemmers =
        ModuleClassDiscovery.getClassesForPackage("org.tartarus.snowball.ext").stream()
            .filter(c -> c.getName().endsWith("Stemmer"))
            .map(stemmerCast)
            .sorted(Comparator.comparing(Class::getName))
            .collect(Collectors.toList());
    if (VERBOSE) {
      System.out.println("snowballStemmers = " + snowballStemmers);
    }
  }

  @AfterClass
  public static void afterClass() {
    tokenizers = null;
    tokenfilters = null;
    charfilters = null;
    snowballStemmers = null;
  }

  /** Creates a static/unmodifiable set from 2 collections as union. */
  private static <T> Set<T> union(Collection<T> c1, Collection<T> c2) {
    return Stream.concat(c1.stream(), c2.stream()).collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Hack to work around the stupidness of Oracle's strict Java backwards compatibility. {@code
   * Class<T>#getConstructors()} should return unmodifiable {@code List<Constructor<T>>} not array!
   */
  @SuppressWarnings("unchecked")
  private static <T> Constructor<T> castConstructor(Class<T> instanceClazz, Constructor<?> ctor) {
    return (Constructor<T>) ctor;
  }

  @SuppressWarnings("unchecked")
  static <T> T newRandomArg(Random random, Class<T> paramType) {
    // if the argument type is not a primitive, return 1/10th of all cases null:
    if (!paramType.isPrimitive() && random.nextInt(10) == 0) {
      return null;
    }
    final Function<Random, Object> producer = argProducers.get(paramType);
    assertNotNull("No producer for arguments of type " + paramType.getName() + " found", producer);
    return (T) producer.apply(random);
  }

  static Object[] newTokenizerArgs(Random random, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      args[i] = newRandomArg(random, paramType);
    }
    return args;
  }

  static Object[] newCharFilterArgs(Random random, Reader reader, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      if (paramType == Reader.class) {
        args[i] = reader;
      } else {
        args[i] = newRandomArg(random, paramType);
      }
    }
    return args;
  }

  static Object[] newFilterArgs(Random random, TokenStream stream, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      if (paramType == TokenStream.class) {
        args[i] = stream;
      } else if (paramType == CommonGramsFilter.class) {
        // TODO: fix this one, thats broken: CommonGramsQueryFilter takes this one explicitly
        args[i] = new CommonGramsFilter(stream, newRandomArg(random, CharArraySet.class));
      } else {
        args[i] = newRandomArg(random, paramType);
      }
    }
    return args;
  }

  static class MockRandomAnalyzer extends Analyzer {
    final long seed;

    MockRandomAnalyzer(long seed) {
      this.seed = seed;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Random random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random);
      // System.out.println("seed=" + seed + ",create tokenizer=" + tokenizerSpec.toString);
      TokenFilterSpec filterSpec = newFilterChain(random, tokenizerSpec.tokenizer);
      // System.out.println("seed=" + seed + ",create filter=" + filterSpec.toString);
      return new TokenStreamComponents(tokenizerSpec.tokenizer, filterSpec.stream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      Random random = new Random(seed);
      CharFilterSpec charfilterspec = newCharFilterChain(random, reader);
      return charfilterspec.reader;
    }

    @Override
    public String toString() {
      Random random = new Random(seed);
      StringBuilder sb = new StringBuilder();
      CharFilterSpec charFilterSpec = newCharFilterChain(random, new StringReader(""));
      sb.append("\ncharfilters=");
      sb.append(charFilterSpec.toString);
      // intentional: initReader gets its own separate random
      random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random);
      sb.append("\n");
      sb.append("tokenizer=");
      sb.append(tokenizerSpec.toString);
      TokenFilterSpec tokenFilterSpec = newFilterChain(random, tokenizerSpec.tokenizer);
      sb.append("\n");
      sb.append("filters=");
      sb.append(tokenFilterSpec.toString);
      return sb.toString();
    }

    private <T> T createComponent(
        Constructor<T> ctor, Object[] args, StringBuilder descr, boolean isConditional) {
      try {
        final T instance = ctor.newInstance(args);
        /*
        if (descr.length() > 0) {
          descr.append(",");
        }
        */
        descr.append("\n  ");
        if (isConditional) {
          descr.append("Conditional:");
        }
        descr.append(ctor.getDeclaringClass().getName());
        String params = Arrays.deepToString(args);
        params = params.substring(1, params.length() - 1);
        descr.append("(").append(params).append(")");
        return instance;
      } catch (InvocationTargetException ite) {
        final Throwable cause = ite.getCause();
        if (cause instanceof IllegalArgumentException
            || (cause instanceof NullPointerException && Stream.of(args).anyMatch(Objects::isNull))
            || cause instanceof UnsupportedOperationException) {
          // thats ok, ignore
          if (VERBOSE) {
            System.err.println("Ignoring IAE/UOE/NPE from ctor:");
            cause.printStackTrace(System.err);
          }
        } else {
          Rethrow.rethrow(cause);
        }
      } catch (IllegalAccessException | InstantiationException iae) {
        Rethrow.rethrow(iae);
      }
      return null; // no success
    }

    private boolean broken(Constructor<?> ctor, Object[] args) {
      final Predicate<Object[]> pred = brokenConstructors.get(ctor);
      return pred != null && pred.test(args);
    }

    // create a new random tokenizer from classpath
    private TokenizerSpec newTokenizer(Random random) {
      TokenizerSpec spec = new TokenizerSpec();
      while (spec.tokenizer == null) {
        final Constructor<? extends Tokenizer> ctor =
            tokenizers.get(random.nextInt(tokenizers.size()));
        final StringBuilder descr = new StringBuilder();
        final Object[] args = newTokenizerArgs(random, ctor.getParameterTypes());
        if (broken(ctor, args)) {
          continue;
        }
        spec.tokenizer = createComponent(ctor, args, descr, false);
        if (spec.tokenizer != null) {
          spec.toString = descr.toString();
        }
      }
      return spec;
    }

    private CharFilterSpec newCharFilterChain(Random random, Reader reader) {
      CharFilterSpec spec = new CharFilterSpec();
      spec.reader = reader;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(3);
      for (int i = 0; i < numFilters; i++) {
        while (true) {
          final Constructor<? extends CharFilter> ctor =
              charfilters.get(random.nextInt(charfilters.size()));
          final Object[] args = newCharFilterArgs(random, spec.reader, ctor.getParameterTypes());
          if (broken(ctor, args)) {
            continue;
          }
          reader = createComponent(ctor, args, descr, false);
          if (reader != null) {
            spec.reader = reader;
            break;
          }
        }
      }
      spec.toString = descr.toString();
      return spec;
    }

    private TokenFilterSpec newFilterChain(Random random, Tokenizer tokenizer) {
      TokenFilterSpec spec = new TokenFilterSpec();
      spec.stream = tokenizer;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(5);
      for (int i = 0; i < numFilters; i++) {

        // Insert ValidatingTF after each stage so we can
        // catch problems right after the TF that "caused"
        // them:
        spec.stream = new ValidatingTokenFilter(spec.stream, "stage " + i);

        while (true) {
          final Constructor<? extends TokenFilter> ctor =
              tokenfilters.get(random.nextInt(tokenfilters.size()));
          if (random.nextBoolean()
              && avoidConditionals.contains(ctor.getDeclaringClass()) == false) {
            long seed = random.nextLong();
            spec.stream =
                new ConditionalTokenFilter(
                    spec.stream,
                    in -> {
                      final Object[] args = newFilterArgs(random, in, ctor.getParameterTypes());
                      if (broken(ctor, args)) {
                        return in;
                      }
                      TokenStream ts = createComponent(ctor, args, descr, true);
                      if (ts == null) {
                        return in;
                      }
                      return ts;
                    }) {
                  Random random = new Random(seed);

                  @Override
                  public void reset() throws IOException {
                    super.reset();
                    random = new Random(seed);
                  }

                  @Override
                  protected boolean shouldFilter() throws IOException {
                    return random.nextBoolean();
                  }
                };
            break;
          } else {
            final Object[] args = newFilterArgs(random, spec.stream, ctor.getParameterTypes());
            if (broken(ctor, args)) {
              continue;
            }
            final TokenFilter flt = createComponent(ctor, args, descr, false);
            if (flt != null) {
              spec.stream = flt;
              break;
            }
          }
        }
      }

      // Insert ValidatingTF after each stage so we can
      // catch problems right after the TF that "caused"
      // them:
      spec.stream = new ValidatingTokenFilter(spec.stream, "last stage");

      spec.toString = descr.toString();
      return spec;
    }
  }

  static class TokenizerSpec {
    Tokenizer tokenizer;
    String toString;
  }

  static class TokenFilterSpec {
    TokenStream stream;
    String toString;
  }

  static class CharFilterSpec {
    Reader reader;
    String toString;
  }

  public void testRandomChains() throws Throwable {
    int numIterations = TEST_NIGHTLY ? atLeast(20) : 3;
    Random random = random();
    for (int i = 0; i < numIterations; i++) {
      try (MockRandomAnalyzer a = new MockRandomAnalyzer(random.nextLong())) {
        if (VERBOSE) {
          System.out.println("Creating random analyzer:" + a);
        }
        try {
          checkNormalize(a);
          checkRandomData(
              random,
              a,
              500 * RANDOM_MULTIPLIER,
              20,
              false,
              false /* We already validate our own offsets... */);
        } catch (Throwable e) {
          System.err.println("Exception from random analyzer: " + a);
          throw e;
        }
      }
    }
  }

  public void checkNormalize(Analyzer a) {
    // normalization should not modify characters that may be used for wildcards
    // or regular expressions
    String s = "([0-9]+)?*";
    assertEquals(s, a.normalize("dummy", s).utf8ToString());
  }

  // we might regret this decision...
  public void testRandomChainsWithLargeStrings() throws Throwable {
    int numIterations = TEST_NIGHTLY ? atLeast(20) : 3;
    Random random = random();
    for (int i = 0; i < numIterations; i++) {
      try (MockRandomAnalyzer a = new MockRandomAnalyzer(random.nextLong())) {
        if (VERBOSE) {
          System.out.println("Creating random analyzer:" + a);
        }
        try {
          checkRandomData(
              random,
              a,
              50 * RANDOM_MULTIPLIER,
              80,
              false,
              false /* We already validate our own offsets... */);
        } catch (Throwable e) {
          System.err.println("Exception from random analyzer: " + a);
          throw e;
        }
      }
    }
  }
}
