<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Apache Lucene Migration Guide

## Migration from Lucene 9.0 to Lucene 9.1

### Test framework package migration and module (LUCENE-10301)

The test framework is now a Java module. All the classes have been moved from
`org.apache.lucene.*` to `org.apache.lucene.tests.*` to avoid package name conflicts
with the core module. If you were using the Lucene test framework, the migration should be
fairly automatic (package prefix).

### Minor syntactical changes in StandardQueryParser (LUCENE-10223)

Added interval functions and min-should-match support to `StandardQueryParser`. This
means that interval function prefixes (`fn:`) and the `@` character after parentheses will
parse differently than before. If you need the exact previous behavior, clone the
`StandardSyntaxParser` from the previous version of Lucene and create a custom query parser
with that parser.

### Lucene Core now depends on java.logging (JUL) module (LUCENE-10342)

Lucene Core now logs certain warnings and errors using Java Util Logging (JUL).
It is therefore recommended to install wrapper libraries with JUL logging handlers to
feed the log events into your app's own logging system.

Under normal circumstances Lucene won't log anything, but in the case of a problem
users should find the logged information in the usual log files.

Lucene also provides a `JavaLoggingInfoStream` implementation that logs `IndexWriter`
events using JUL.

To feed Lucene's log events into the well-known Log4J system, we refer to
the [Log4j JDK Logging Adapter](https://logging.apache.org/log4j/2.x/log4j-jul/index.html)
in combination with the corresponding system property:
`java.util.logging.manager=org.apache.logging.log4j.jul.LogManager`.

### Kuromoji and Nori analysis component constructors for custom dictionaries

The Kuromoji and Nori analysis modules had some way to customize the backing dictionaries
by passing a path to file or classpath resources using some inconsistently implemented
APIs. This was buggy from the beginning, but some users made use of it. Due to move to Java
module system, especially the resource lookup on classpath stopped to work correctly.
The Lucene team therefore implemented new APIs to create dictionary implementations
with custom data files. Unfortunately there were some shortcomings in the 9.1 version,
also when using the now deprecated ctors, so users are advised to upgrade to
Lucene 9.2 or stay with 9.0.

See LUCENE-10558 for more details and workarounds.

## Migration from Lucene 8.x to Lucene 9.0

### Rename of binary artifacts from '**-analyzers-**' to '**-analysis-**' (LUCENE-9562)

All binary analysis packages (and corresponding Maven artifacts) have been renamed and are
now consistent with repository module `analysis`. You will need to adjust build dependencies
to the new coordinates:

|         Old Artifact Coordinates            |        New Artifact Coordinates            |
|---------------------------------------------|--------------------------------------------|
|org.apache.lucene:lucene-analyzers-common    |org.apache.lucene:lucene-analysis-common    |
|org.apache.lucene:lucene-analyzers-icu       |org.apache.lucene:lucene-analysis-icu       |
|org.apache.lucene:lucene-analyzers-kuromoji  |org.apache.lucene:lucene-analysis-kuromoji  |
|org.apache.lucene:lucene-analyzers-morfologik|org.apache.lucene:lucene-analysis-morfologik|
|org.apache.lucene:lucene-analyzers-nori      |org.apache.lucene:lucene-analysis-nori      |
|org.apache.lucene:lucene-analyzers-opennlp   |org.apache.lucene:lucene-analysis-opennlp   |
|org.apache.lucene:lucene-analyzers-phonetic  |org.apache.lucene:lucene-analysis-phonetic  |
|org.apache.lucene:lucene-analyzers-smartcn   |org.apache.lucene:lucene-analysis-smartcn   |
|org.apache.lucene:lucene-analyzers-stempel   |org.apache.lucene:lucene-analysis-stempel   |


### LucenePackage class removed (LUCENE-10260)

`LucenePackage` class has been removed. The implementation string can be
retrieved from `Version.getPackageImplementationVersion()`.

### Directory API is now little-endian (LUCENE-9047)

`DataOutput`'s `writeShort()`, `writeInt()`, and `writeLong()` methods now encode with
little-endian byte order. If you have custom subclasses of `DataInput`/`DataOutput`, you
will need to adjust them from big-endian byte order to little-endian byte order.

### NativeUnixDirectory removed and replaced by DirectIODirectory (LUCENE-8982)

Java 11 supports to use Direct IO without native wrappers from Java code.
`NativeUnixDirectory` in the misc module was therefore removed and replaced
by `DirectIODirectory`. To use it, you need a JVM and operating system that
supports Direct IO.

### BM25Similarity.setDiscountOverlaps and LegacyBM25Similarity.setDiscountOverlaps methods removed (LUCENE-9646)

The `discountOverlaps()` parameter for both `BM25Similarity` and `LegacyBM25Similarity`
is now set by the constructor of those classes.

### Packages in misc module are renamed (LUCENE-9600)

These packages in the `lucene-misc` module are renamed:

|    Old Package Name      |       New Package Name        |
|--------------------------|-------------------------------|
|org.apache.lucene.document|org.apache.lucene.misc.document|
|org.apache.lucene.index   |org.apache.lucene.misc.index   |
|org.apache.lucene.search  |org.apache.lucene.misc.search  |
|org.apache.lucene.store   |org.apache.lucene.misc.store   |
|org.apache.lucene.util    |org.apache.lucene.misc.util    |

The following classes were moved to the `lucene-core` module:

- org.apache.lucene.document.InetAddressPoint
- org.apache.lucene.document.InetAddressRange

### Packages in sandbox module are renamed (LUCENE-9319)

These packages in the `lucene-sandbox` module are renamed:

|    Old Package Name      |       New Package Name           |
|--------------------------|----------------------------------|
|org.apache.lucene.codecs  |org.apache.lucene.sandbox.codecs  |
|org.apache.lucene.document|org.apache.lucene.sandbox.document|
|org.apache.lucene.search  |org.apache.lucene.sandbox.search  |

### Backward codecs are renamed (LUCENE-9318)

These packages in the `lucene-backwards-codecs` module are renamed:

|    Old Package Name    |       New Package Name          |
|------------------------|---------------------------------|
|org.apache.lucene.codecs|org.apache.lucene.backward_codecs|

### JapanesePartOfSpeechStopFilterFactory loads default stop tags if "tags" argument not specified (LUCENE-9567)

Previously, `JapanesePartOfSpeechStopFilterFactory` added no filter if `args` didn't include "tags". Now, it will load 
the default stop tags returned by `JapaneseAnalyzer.getDefaultStopTags()` (i.e. the tags from`stoptags.txt` in the 
`lucene-analyzers-kuromoji` jar.)

### ICUCollationKeyAnalyzer is renamed (LUCENE-9558)

These packages in the `lucene-analysis-icu` module are renamed:

|    Old Package Name       |       New Package Name       |
|---------------------------|------------------------------|
|org.apache.lucene.collation|org.apache.lucene.analysis.icu|

### Base and concrete analysis factories are moved / package renamed (LUCENE-9317)

Base analysis factories are moved to `lucene-core`, also their package names are renamed.

|                Old Class Name                    |               New Class Name               |
|--------------------------------------------------|--------------------------------------------|
|org.apache.lucene.analysis.util.TokenizerFactory  |org.apache.lucene.analysis.TokenizerFactory |
|org.apache.lucene.analysis.util.CharFilterFactory |org.apache.lucene.analysis.CharFilterFactory|
|org.apache.lucene.analysis.util.TokenFilterFactory|org.apache.lucene.analysis.TokenizerFactory |

The service provider files placed in `META-INF/services` for custom analysis factories should be renamed as follows:

- META-INF/services/org.apache.lucene.analysis.TokenizerFactory
- META-INF/services/org.apache.lucene.analysis.CharFilterFactory
- META-INF/services/org.apache.lucene.analysis.TokenFilterFactory

`StandardTokenizerFactory` is moved to `lucene-core` module.

The `org.apache.lucene.analysis.standard` package in `lucene-analysis-common` module
is split into `org.apache.lucene.analysis.classic` and `org.apache.lucene.analysis.email`.

### RegExpQuery now rejects invalid backslashes (LUCENE-9370)

We now follow the [Java rules](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#bs) for accepting backslashes. 
Alphabetic characters other than s, S, w, W, d or D that are preceded by a backslash are considered illegal syntax and will throw an exception.  

### RegExp certain regular expressions now match differently (LUCENE-9336)

The commonly used regular expressions \w \W \d \D \s and \S now work the same way [Java Pattern](https://docs.oracle.com/javase/tutorial/essential/regex/pre_char_classes.html#CHART) matching works. Previously these expressions were (mis)interpreted as searches for the literal characters w, d, s etc. 

### NGramFilterFactory "keepShortTerm" option was fixed to "preserveOriginal" (LUCENE-9259)

The factory option name to output the original term was corrected in accordance with its Javadoc.

### IndexMergeTool defaults changes (LUCENE-9206)

This command-line tool no longer forceMerges to a single segment. Instead, by
default it just follows (configurable) merge policy. If you really want to merge
to a single segment, you can pass `-max-segments 1`.

### FST Builder is renamed FSTCompiler with fluent-style Builder (LUCENE-9089)

Simply use `FSTCompiler` instead of the previous `Builder`. Use either the simple constructor with default settings, or
the `FSTCompiler.Builder` to tune and tweak any parameter.

### Kuromoji user dictionary now forbids illegal segmentation (LUCENE-8933)

User dictionary now strictly validates if the (concatenated) segment is the same as the surface form. This change avoids
unexpected runtime exceptions or behaviours.
For example, these entries are not allowed at all and an exception is thrown when loading the dictionary file.

```
# concatenated "日本経済新聞" does not match the surface form "日経新聞"
日経新聞,日本 経済 新聞,ニホン ケイザイ シンブン,カスタム名詞

# concatenated "日経新聞" does not match the surface form "日本経済新聞"
日本経済新聞,日経 新聞,ニッケイ シンブン,カスタム名詞
```

### JapaneseTokenizer no longer emits original (compound) tokens by default when the mode is not NORMAL (LUCENE-9123)

`JapaneseTokenizer` and `JapaneseAnalyzer` no longer emits original tokens when `discardCompoundToken` option is not specified.
The constructor option has been introduced since Lucene 8.5.0, and the default value is changed to `true`.

When given the text "株式会社", JapaneseTokenizer (mode != NORMAL) emits decompounded tokens "株式" and "会社" only and no
longer outputs the original token "株式会社" by default. To output original tokens, `discardCompoundToken` option should be
explicitly set to `false`. Be aware that if this option is set to `false`, `SynonymFilter` or `SynonymGraphFilter` does not work
correctly (see LUCENE-9173).

### Analysis factories now have customizable symbolic names (LUCENE-8778) and need additional no-arg constructor (LUCENE-9281)

The SPI names for concrete subclasses of `TokenizerFactory`, `TokenFilterFactory`, and `CharfilterFactory` are no longer
derived from their class name. Instead, each factory must have a static "NAME" field like this:

```java
    /** o.a.l.a.standard.StandardTokenizerFactory's SPI name */
    public static final String NAME = "standard";
```

A factory can be resolved/instantiated with its `NAME` by using methods such as `TokenizerFactory.lookupClass(String)`
or `TokenizerFactory.forName(String, Map<String,String>)`.

If there are any user-defined factory classes that don't have proper `NAME` field, an exception will be thrown
when (re)loading factories. e.g., when calling `TokenizerFactory.reloadTokenizers(ClassLoader)`.

In addition starting all factories need to implement a public no-arg constructor, too. The reason for this
change comes from the fact that Lucene now uses `java.util.ServiceLoader` instead its own implementation to
load the factory classes to be compatible with Java Module System changes (e.g., load factories from modules).
In the future, extensions to Lucene developed on the Java Module System may expose the factories from their
`module-info.java` file instead of `META-INF/services`.

This constructor is never called by Lucene, so by default it throws an `UnsupportedOperationException`. User-defined
factory classes should implement it in the following way:

```java
    /** Default ctor for compatibility with SPI */
    public StandardTokenizerFactory() {
      throw defaultCtorException();
    }
```

(`defaultCtorException()` is a protected static helper method)

### TermsEnum is now fully abstract (LUCENE-8292, LUCENE-8662)

`TermsEnum` has been changed to be fully abstract, so non-abstract subclasses must implement all its methods.
Non-Performance critical `TermsEnum`s can use `BaseTermsEnum` as a base class instead. The change was motivated
by several performance issues with `FilterTermsEnum` that caused significant slowdowns and massive memory consumption due
to not delegating all method from `TermsEnum`.

### RAMDirectory, RAMFile, RAMInputStream, RAMOutputStream removed (LUCENE-8474)

RAM-based directory implementation have been removed.
`ByteBuffersDirectory` can be used as a RAM-resident replacement, although it
is discouraged in favor of the default `MMapDirectory`.

### Similarity.SimScorer.computeXXXFactor methods removed (LUCENE-8014)

`SpanQuery` and `PhraseQuery` now always calculate their slops as
`(1.0 / (1.0 + distance))`.  Payload factor calculation is performed by
`PayloadDecoder` in the `lucene-queries` module.

### Scorer must produce positive scores (LUCENE-7996)

`Scorer`s are no longer allowed to produce negative scores. If you have custom
query implementations, you should make sure their score formula may never produce
negative scores.

As a side-effect of this change, negative boosts are now rejected and
`FunctionScoreQuery` maps negative values to 0.

### CustomScoreQuery, BoostedQuery and BoostingQuery removed (LUCENE-8099)

Instead use `FunctionScoreQuery` and a `DoubleValuesSource` implementation.  `BoostedQuery`
and `BoostingQuery` may be replaced by calls to `FunctionScoreQuery.boostByValue()` and
`FunctionScoreQuery.boostByQuery()`.  To replace more complex calculations in
`CustomScoreQuery`, use the `lucene-expressions` module:

```java
SimpleBindings bindings = new SimpleBindings();
bindings.add("score", DoubleValuesSource.SCORES);
bindings.add("boost1", DoubleValuesSource.fromIntField("myboostfield"));
bindings.add("boost2", DoubleValuesSource.fromIntField("myotherboostfield"));
Expression expr = JavascriptCompiler.compile("score * (boost1 + ln(boost2))");
FunctionScoreQuery q = new FunctionScoreQuery(inputQuery, expr.getDoubleValuesSource(bindings));
```

### IndexOptions can no longer be changed dynamically (LUCENE-8134)

Changing `IndexOptions` for a field on the fly will now result into an
`IllegalArgumentException`. If a field is indexed
(`FieldType.indexOptions() != IndexOptions.NONE`) then all documents must have
the same index options for that field.


### IndexSearcher.createNormalizedWeight() removed (LUCENE-8242)

Instead use `IndexSearcher.createWeight()`, rewriting the query first, and using
a boost of `1f`.

### Memory codecs removed (LUCENE-8267)

Memory codecs (`MemoryPostingsFormat`, `MemoryDocValuesFormat`) have been removed from the codebase.

### Direct doc-value format removed (LUCENE-8917)

The `Direct` doc-value format has been removed from the codebase.

### QueryCachingPolicy.ALWAYS_CACHE removed (LUCENE-8144)

Caching everything is discouraged as it disables the ability to skip non-interesting documents.
`ALWAYS_CACHE` can be replaced by a `UsageTrackingQueryCachingPolicy` with an appropriate config.

### English stopwords are no longer removed by default in StandardAnalyzer (LUCENE-7444)

To retain the old behaviour, pass `EnglishAnalyzer.ENGLISH_STOP_WORDS_SET` as an argument
to the constructor

### StandardAnalyzer.ENGLISH_STOP_WORDS_SET has been moved

English stop words are now defined in `EnglishAnalyzer.ENGLISH_STOP_WORDS_SET` in the
`analysis-common` module.

### TopDocs.maxScore removed

`TopDocs.maxScore` is removed. `IndexSearcher` and `TopFieldCollector` no longer have
an option to compute the maximum score when sorting by field. If you need to
know the maximum score for a query, the recommended approach is to run a
separate query:

```java
  TopDocs topHits = searcher.search(query, 1);
  float maxScore = topHits.scoreDocs.length == 0 ? Float.NaN : topHits.scoreDocs[0].score;
```

Thanks to other optimizations that were added to Lucene 8, this query will be
able to efficiently select the top-scoring document without having to visit
all matches.

### TopFieldCollector always assumes fillFields=true

Because filling sort values doesn't have a significant overhead, the `fillFields`
option has been removed from `TopFieldCollector` factory methods. Everything
behaves as if it was previously set to `true`.

### TopFieldCollector no longer takes a trackDocScores option

Computing scores at collection time is less efficient than running a second
request in order to only compute scores for documents that made it to the top
hits. As a consequence, the `trackDocScores` option has been removed and can be
replaced with the new `TopFieldCollector.populateScores()` helper method.

### IndexSearcher.search(After) may return lower bounds of the hit count and TopDocs.totalHits is no longer a long

Lucene 8 received optimizations for collection of top-k matches by not visiting
all matches. However these optimizations won't help if all matches still need
to be visited in order to compute the total number of hits. As a consequence,
`IndexSearcher`'s `search()` and `searchAfter()` methods were changed to only count hits
accurately up to 1,000, and `Topdocs.totalHits` was changed from a `long` to an
object that says whether the hit count is accurate or a lower bound of the
actual hit count.

### RAMDirectory, RAMFile, RAMInputStream, RAMOutputStream are deprecated (LUCENE-8467, LUCENE-8438)

This RAM-based directory implementation is an old piece of code that uses inefficient
thread synchronization primitives and can be confused as "faster" than the NIO-based
`MMapDirectory`. It is deprecated and scheduled for removal in future versions of 
Lucene.

### LeafCollector.setScorer() now takes a Scorable rather than a Scorer (LUCENE-6228)

`Scorer` has a number of methods that should never be called from `Collector`s, for example
those that advance the underlying iterators.  To hide these, `LeafCollector.setScorer()`
now takes a `Scorable`, an abstract class that scorers can extend, with methods
`docId()` and `score()`.

### Scorers must have non-null Weights

If a custom `Scorer` implementation does not have an associated `Weight`, it can probably
be replaced with a `Scorable` instead.

### Suggesters now return Long instead of long for weight() during indexing, and double instead of long at suggest time 

Most code should just require recompilation, though possibly requiring some added casts.

### TokenStreamComponents is now final

Instead of overriding `TokenStreamComponents.setReader()` to customise analyzer
initialisation, you should now pass a `Consumer<Reader>` instance to the
`TokenStreamComponents` constructor.

### LowerCaseTokenizer and LowerCaseTokenizerFactory have been removed

`LowerCaseTokenizer` combined tokenization and filtering in a way that broke token
normalization, so they have been removed. Instead, use a `LetterTokenizer` followed by
a `LowerCaseFilter`.

### CharTokenizer no longer takes a normalizer function

`CharTokenizer` now only performs tokenization. To perform any type of filtering
use a `TokenFilter` chain as you would with any other `Tokenizer`.

### Highlighter and FastVectorHighlighter no longer support ToParent/ToChildBlockJoinQuery

Both `Highlighter` and `FastVectorHighlighter` need a custom `WeightedSpanTermExtractor` or `FieldQuery`, respectively,
in order to support `ToParentBlockJoinQuery`/`ToChildBlockJoinQuery`.

### MultiTermAwareComponent replaced by CharFilterFactory.normalize() and TokenFilterFactory.normalize()

Normalization is now type-safe, with `CharFilterFactory.normalize()` returning a `Reader` and
`TokenFilterFactory.normalize()` returning a `TokenFilter`.

### k1+1 constant factor removed from BM25 similarity numerator (LUCENE-8563)

Scores computed by the `BM25Similarity` are lower than previously as the `k1+1`
constant factor was removed from the numerator of the scoring formula.
Ordering of results is preserved unless scores are computed from multiple
fields using different similarities. The previous behaviour is now exposed
by the `LegacyBM25Similarity` class which can be found in the lucene-misc jar.

### IndexWriter.maxDoc()/numDocs() removed in favor of IndexWriter.getDocStats()

`IndexWriter.getDocStats()` should be used instead of `maxDoc()` / `numDocs()` which offers a consistent 
view on document stats. Previously calling two methods in order to get point in time stats was subject
to concurrent changes.

### maxClausesCount moved from BooleanQuery To IndexSearcher (LUCENE-8811)

`IndexSearcher` now performs max clause count checks on all types of queries (including BooleanQueries).
This led to a logical move of the clauses count from `BooleanQuery` to `IndexSearcher`.

### TopDocs.merge shall no longer allow setting of shard indices

`TopDocs.merge()`'s API has been changed to stop allowing passing in a parameter to indicate if it should
set shard indices for hits as they are seen during the merge process. This is done to simplify the API
to be more dynamic in terms of passing in custom tie breakers.
If shard indices are to be used for tie breaking docs with equal scores during `TopDocs.merge()`, then it is
mandatory that the input `ScoreDocs` have their shard indices set to valid values prior to calling `merge()`

### TopDocsCollector Shall Throw IllegalArgumentException For Malformed Arguments

`TopDocsCollector` shall no longer return an empty `TopDocs` for malformed arguments.
Rather, an `IllegalArgumentException` shall be thrown. This is introduced for better
defence and to ensure that there is no bubbling up of errors when Lucene is
used in multi level applications

### Assumption of data consistency between different data-structures sharing the same field name

Sorting on a numeric field that is indexed with both doc values and points may use an
optimization to skip non-competitive documents. This optimization relies on the assumption
that the same data is stored in these points and doc values.

### Require consistency between data-structures on a per-field basis

The per field data-structures are implicitly defined by the first document 
indexed that contains a certain field. Once defined, the per field 
data-structures are not changeable for the whole index. For example, if you 
first index a document where a certain field is indexed with doc values and 
points, all subsequent documents containing this field must also have this
field indexed with only doc values and points.

This also means that an index created in the previous version that doesn't 
satisfy this requirement can not be updated. 

### Doc values updates are allowed only for doc values only fields

Previously IndexWriter could update doc values for a binary or numeric docValue 
field that was also indexed with other data structures (e.g. postings, vectors 
etc). This is not allowed anymore. A field must be indexed with only doc values 
to be allowed for doc values updates in `IndexWriter`.

### SortedDocValues no longer extends BinaryDocValues (LUCENE-9796)

`SortedDocValues` no longer extends `BinaryDocValues`: `SortedDocValues` do not have a per-document
binary value, they have a per-document numeric `ordValue()`. The ordinal can then be dereferenced
to its binary form with `lookupOrd()`, but it was a performance trap to implement a `binaryValue()`
on the SortedDocValues api that does this behind-the-scenes on every document.

You can replace calls of `binaryValue()` with `lookupOrd(ordValue())` as a "quick fix", but it is
better to use the ordinal alone (integer-based datastructures) for per-document access, and only
call `lookupOrd()` a few times at the end (e.g. for the hits you want to display). Otherwise, if you
really don't want per-document ordinals, but instead a per-document `byte[]`, use a `BinaryDocValues`
field.

### Removed CodecReader.ramBytesUsed() (LUCENE-9387)

Lucene index readers are now using so little memory with the default codec that
it was decided to remove the ability to estimate their RAM usage.

### LongValueFacetCounts no longer accepts multiValued param in constructors (LUCENE-9948)

`LongValueFacetCounts` will now automatically detect whether-or-not an indexed field is single- or
multi-valued. The user no longer needs to provide this information to the ctors. Migrating should
be as simple as no longer providing this boolean.

### SpanQuery and subclasses have moved from core/ to the queries module

They can now be found in the `org.apache.lucene.queries.spans` package.

### SpanBoostQuery has been removed (LUCENE-8143)

`SpanBoostQuery` was a no-op unless used at the top level of a `SpanQuery` nested
structure. Use a standard `BoostQuery` here instead.

### Sort is immutable (LUCENE-9325)

Rather than using `setSort()` to change sort values, you should instead create
a new `Sort` instance with the new values.

### Taxonomy-based faceting uses more modern encodings (LUCENE-9450, LUCENE-10062, LUCENE-10122)

The side-car taxonomy index now uses doc values for ord-to-path lookup (LUCENE-9450) and parent
lookup (LUCENE-10122) instead of stored fields and positions (respectively). Document ordinals
are now encoded with `SortedNumericDocValues` instead of using a custom (v-int) binary format.
Performance gains have been observed with these encoding changes, but to benefit from them, users
must create a new index using 9.x (it is not sufficient to reindex documents against an existing
8.x index). In order to remain backwards-compatible with 8.x indexes, the older format is retained 
until a full rebuild is done.

Additionally, `OrdinalsReader` (and sub-classes) have been marked `@Deprecated` as custom binary
encodings will not be supported for Document ordinals in 9.x onwards (`SortedNumericDocValues` are
used out-of-the-box instead).
