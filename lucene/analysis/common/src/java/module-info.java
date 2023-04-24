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

/** Lucene Analysis Common. */
module org.apache.lucene.analysis.common {
  requires java.xml;
  requires org.apache.lucene.core;

  exports org.apache.lucene.analysis.ar;
  exports org.apache.lucene.analysis.bg;
  exports org.apache.lucene.analysis.bn;
  exports org.apache.lucene.analysis.boost;
  exports org.apache.lucene.analysis.br;
  exports org.apache.lucene.analysis.ca;
  exports org.apache.lucene.analysis.charfilter;
  exports org.apache.lucene.analysis.cjk;
  exports org.apache.lucene.analysis.ckb;
  exports org.apache.lucene.analysis.classic;
  exports org.apache.lucene.analysis.commongrams;
  exports org.apache.lucene.analysis.compound.hyphenation;
  exports org.apache.lucene.analysis.compound;
  exports org.apache.lucene.analysis.core;
  exports org.apache.lucene.analysis.custom;
  exports org.apache.lucene.analysis.cz;
  exports org.apache.lucene.analysis.da;
  exports org.apache.lucene.analysis.de;
  exports org.apache.lucene.analysis.el;
  exports org.apache.lucene.analysis.email;
  exports org.apache.lucene.analysis.en;
  exports org.apache.lucene.analysis.es;
  exports org.apache.lucene.analysis.et;
  exports org.apache.lucene.analysis.eu;
  exports org.apache.lucene.analysis.fa;
  exports org.apache.lucene.analysis.fi;
  exports org.apache.lucene.analysis.fr;
  exports org.apache.lucene.analysis.ga;
  exports org.apache.lucene.analysis.gl;
  exports org.apache.lucene.analysis.hi;
  exports org.apache.lucene.analysis.hu;
  exports org.apache.lucene.analysis.hunspell;
  exports org.apache.lucene.analysis.hy;
  exports org.apache.lucene.analysis.id;
  exports org.apache.lucene.analysis.in;
  exports org.apache.lucene.analysis.it;
  exports org.apache.lucene.analysis.lt;
  exports org.apache.lucene.analysis.lv;
  exports org.apache.lucene.analysis.minhash;
  exports org.apache.lucene.analysis.miscellaneous;
  exports org.apache.lucene.analysis.ne;
  exports org.apache.lucene.analysis.ngram;
  exports org.apache.lucene.analysis.nl;
  exports org.apache.lucene.analysis.no;
  exports org.apache.lucene.analysis.path;
  exports org.apache.lucene.analysis.pattern;
  exports org.apache.lucene.analysis.payloads;
  exports org.apache.lucene.analysis.pt;
  exports org.apache.lucene.analysis.query;
  exports org.apache.lucene.analysis.reverse;
  exports org.apache.lucene.analysis.ro;
  exports org.apache.lucene.analysis.ru;
  exports org.apache.lucene.analysis.shingle;
  exports org.apache.lucene.analysis.sinks;
  exports org.apache.lucene.analysis.snowball;
  exports org.apache.lucene.analysis.sr;
  exports org.apache.lucene.analysis.sv;
  exports org.apache.lucene.analysis.synonym;
  exports org.apache.lucene.analysis.synonym.word2vec;
  exports org.apache.lucene.analysis.ta;
  exports org.apache.lucene.analysis.te;
  exports org.apache.lucene.analysis.th;
  exports org.apache.lucene.analysis.tr;
  exports org.apache.lucene.analysis.util;
  exports org.apache.lucene.analysis.wikipedia;
  exports org.apache.lucene.collation.tokenattributes;
  exports org.apache.lucene.collation;
  exports org.tartarus.snowball.ext;
  exports org.tartarus.snowball;

  opens org.apache.lucene.analysis.ar to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.bg to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.bn to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.br to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ca to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.cjk to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ckb to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.cz to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.el to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.et to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.eu to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.fa to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ga to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.gl to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.hi to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.hy to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.id to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.lt to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.lv to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ne to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ro to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.snowball to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.sr to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ta to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.te to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.th to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.tr to
      org.apache.lucene.core;

  provides org.apache.lucene.analysis.CharFilterFactory with
      org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory,
      org.apache.lucene.analysis.charfilter.MappingCharFilterFactory,
      org.apache.lucene.analysis.cjk.CJKWidthCharFilterFactory,
      org.apache.lucene.analysis.fa.PersianCharFilterFactory,
      org.apache.lucene.analysis.pattern.PatternReplaceCharFilterFactory;
  provides org.apache.lucene.analysis.TokenFilterFactory with
      org.apache.lucene.analysis.tr.ApostropheFilterFactory,
      org.apache.lucene.analysis.ar.ArabicNormalizationFilterFactory,
      org.apache.lucene.analysis.ar.ArabicStemFilterFactory,
      org.apache.lucene.analysis.bg.BulgarianStemFilterFactory,
      org.apache.lucene.analysis.boost.DelimitedBoostTokenFilterFactory,
      org.apache.lucene.analysis.bn.BengaliNormalizationFilterFactory,
      org.apache.lucene.analysis.bn.BengaliStemFilterFactory,
      org.apache.lucene.analysis.br.BrazilianStemFilterFactory,
      org.apache.lucene.analysis.cjk.CJKBigramFilterFactory,
      org.apache.lucene.analysis.cjk.CJKWidthFilterFactory,
      org.apache.lucene.analysis.ckb.SoraniNormalizationFilterFactory,
      org.apache.lucene.analysis.ckb.SoraniStemFilterFactory,
      org.apache.lucene.analysis.classic.ClassicFilterFactory,
      org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory,
      org.apache.lucene.analysis.commongrams.CommonGramsQueryFilterFactory,
      org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilterFactory,
      org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilterFactory,
      org.apache.lucene.analysis.core.DecimalDigitFilterFactory,
      org.apache.lucene.analysis.core.LowerCaseFilterFactory,
      org.apache.lucene.analysis.core.StopFilterFactory,
      org.apache.lucene.analysis.core.TypeTokenFilterFactory,
      org.apache.lucene.analysis.core.UpperCaseFilterFactory,
      org.apache.lucene.analysis.cz.CzechStemFilterFactory,
      org.apache.lucene.analysis.de.GermanLightStemFilterFactory,
      org.apache.lucene.analysis.de.GermanMinimalStemFilterFactory,
      org.apache.lucene.analysis.de.GermanNormalizationFilterFactory,
      org.apache.lucene.analysis.de.GermanStemFilterFactory,
      org.apache.lucene.analysis.el.GreekLowerCaseFilterFactory,
      org.apache.lucene.analysis.el.GreekStemFilterFactory,
      org.apache.lucene.analysis.en.EnglishMinimalStemFilterFactory,
      org.apache.lucene.analysis.en.EnglishPossessiveFilterFactory,
      org.apache.lucene.analysis.en.KStemFilterFactory,
      org.apache.lucene.analysis.en.PorterStemFilterFactory,
      org.apache.lucene.analysis.es.SpanishLightStemFilterFactory,
      org.apache.lucene.analysis.es.SpanishMinimalStemFilterFactory,
      org.apache.lucene.analysis.es.SpanishPluralStemFilterFactory,
      org.apache.lucene.analysis.fa.PersianNormalizationFilterFactory,
      org.apache.lucene.analysis.fa.PersianStemFilterFactory,
      org.apache.lucene.analysis.fi.FinnishLightStemFilterFactory,
      org.apache.lucene.analysis.fr.FrenchLightStemFilterFactory,
      org.apache.lucene.analysis.fr.FrenchMinimalStemFilterFactory,
      org.apache.lucene.analysis.ga.IrishLowerCaseFilterFactory,
      org.apache.lucene.analysis.gl.GalicianMinimalStemFilterFactory,
      org.apache.lucene.analysis.gl.GalicianStemFilterFactory,
      org.apache.lucene.analysis.hi.HindiNormalizationFilterFactory,
      org.apache.lucene.analysis.hi.HindiStemFilterFactory,
      org.apache.lucene.analysis.hu.HungarianLightStemFilterFactory,
      org.apache.lucene.analysis.hunspell.HunspellStemFilterFactory,
      org.apache.lucene.analysis.id.IndonesianStemFilterFactory,
      org.apache.lucene.analysis.in.IndicNormalizationFilterFactory,
      org.apache.lucene.analysis.it.ItalianLightStemFilterFactory,
      org.apache.lucene.analysis.lv.LatvianStemFilterFactory,
      org.apache.lucene.analysis.minhash.MinHashFilterFactory,
      org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory,
      org.apache.lucene.analysis.miscellaneous.CapitalizationFilterFactory,
      org.apache.lucene.analysis.miscellaneous.CodepointCountFilterFactory,
      org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilterFactory,
      org.apache.lucene.analysis.miscellaneous.DateRecognizerFilterFactory,
      org.apache.lucene.analysis.miscellaneous.DelimitedTermFrequencyTokenFilterFactory,
      org.apache.lucene.analysis.miscellaneous.DropIfFlaggedFilterFactory,
      org.apache.lucene.analysis.miscellaneous.FingerprintFilterFactory,
      org.apache.lucene.analysis.miscellaneous.FixBrokenOffsetsFilterFactory,
      org.apache.lucene.analysis.miscellaneous.HyphenatedWordsFilterFactory,
      org.apache.lucene.analysis.miscellaneous.KeepWordFilterFactory,
      org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory,
      org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilterFactory,
      org.apache.lucene.analysis.miscellaneous.LengthFilterFactory,
      org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory,
      org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilterFactory,
      org.apache.lucene.analysis.miscellaneous.LimitTokenPositionFilterFactory,
      org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory,
      org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilterFactory,
      org.apache.lucene.analysis.miscellaneous.ProtectedTermFilterFactory,
      org.apache.lucene.analysis.miscellaneous.TrimFilterFactory,
      org.apache.lucene.analysis.miscellaneous.TruncateTokenFilterFactory,
      org.apache.lucene.analysis.miscellaneous.TypeAsSynonymFilterFactory,
      org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory,
      org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilterFactory,
      org.apache.lucene.analysis.miscellaneous.ScandinavianFoldingFilterFactory,
      org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilterFactory,
      org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory,
      org.apache.lucene.analysis.ngram.NGramFilterFactory,
      org.apache.lucene.analysis.no.NorwegianLightStemFilterFactory,
      org.apache.lucene.analysis.no.NorwegianMinimalStemFilterFactory,
      org.apache.lucene.analysis.no.NorwegianNormalizationFilterFactory,
      org.apache.lucene.analysis.pattern.PatternReplaceFilterFactory,
      org.apache.lucene.analysis.pattern.PatternCaptureGroupFilterFactory,
      org.apache.lucene.analysis.pattern.PatternTypingFilterFactory,
      org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory,
      org.apache.lucene.analysis.payloads.NumericPayloadTokenFilterFactory,
      org.apache.lucene.analysis.payloads.TokenOffsetPayloadTokenFilterFactory,
      org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilterFactory,
      org.apache.lucene.analysis.pt.PortugueseLightStemFilterFactory,
      org.apache.lucene.analysis.pt.PortugueseMinimalStemFilterFactory,
      org.apache.lucene.analysis.pt.PortugueseStemFilterFactory,
      org.apache.lucene.analysis.reverse.ReverseStringFilterFactory,
      org.apache.lucene.analysis.ru.RussianLightStemFilterFactory,
      org.apache.lucene.analysis.shingle.ShingleFilterFactory,
      org.apache.lucene.analysis.shingle.FixedShingleFilterFactory,
      org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory,
      org.apache.lucene.analysis.sr.SerbianNormalizationFilterFactory,
      org.apache.lucene.analysis.sv.SwedishLightStemFilterFactory,
      org.apache.lucene.analysis.sv.SwedishMinimalStemFilterFactory,
      org.apache.lucene.analysis.synonym.SynonymFilterFactory,
      org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory,
      org.apache.lucene.analysis.synonym.word2vec.Word2VecSynonymFilterFactory,
      org.apache.lucene.analysis.core.FlattenGraphFilterFactory,
      org.apache.lucene.analysis.te.TeluguNormalizationFilterFactory,
      org.apache.lucene.analysis.te.TeluguStemFilterFactory,
      org.apache.lucene.analysis.tr.TurkishLowerCaseFilterFactory,
      org.apache.lucene.analysis.util.ElisionFilterFactory;
  provides org.apache.lucene.analysis.TokenizerFactory with
      org.apache.lucene.analysis.classic.ClassicTokenizerFactory,
      org.apache.lucene.analysis.core.KeywordTokenizerFactory,
      org.apache.lucene.analysis.core.LetterTokenizerFactory,
      org.apache.lucene.analysis.core.WhitespaceTokenizerFactory,
      org.apache.lucene.analysis.email.UAX29URLEmailTokenizerFactory,
      org.apache.lucene.analysis.ngram.EdgeNGramTokenizerFactory,
      org.apache.lucene.analysis.ngram.NGramTokenizerFactory,
      org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory,
      org.apache.lucene.analysis.pattern.PatternTokenizerFactory,
      org.apache.lucene.analysis.pattern.SimplePatternSplitTokenizerFactory,
      org.apache.lucene.analysis.pattern.SimplePatternTokenizerFactory,
      org.apache.lucene.analysis.th.ThaiTokenizerFactory,
      org.apache.lucene.analysis.wikipedia.WikipediaTokenizerFactory;
}
