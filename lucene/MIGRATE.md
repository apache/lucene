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

## Migration from Lucene 10.x to Lucene 11.0

### TieredMergePolicy#setMaxMergeAtOnce removed

This parameter has no replacement, TieredMergePolicy no longer bounds the
number of segments that may be merged together.

### Query caching is now disabled by default

Query caching is now disabled by default. To enable caching back, do something
like below in a static initialization block:

```
int maxCachedQueries = 1_000;
long maxRamBytesUsed = 50 * 1024 * 1024; // 50MB
IndexSearcher.setDefaultQueryCache(new LRUQueryCache(maxCachedQueries, maxRamBytesUsed));
```

## Migration from Lucene 9.x to Lucene 10.0

### DataInput#readVLong() may now read negative vlongs

LUCENE-10376 started allowing `DataInput#readVLong()` to read negative vlongs.
In particular, this feature is used by the `DataInput#readZLong()` method. A
practical implication is that `DataInput#readVLong()` may now read up to 10
bytes, while it would never read more than 9 bytes in Lucene 9.x.

### Changes to DataInput.readGroupVInt and readGroupVInts methods

As part of GITHUB#13820, GITHUB#13825, GITHUB#13830, this issue corrects DataInput.readGroupVInts
to be public and not-final, allowing subclasses to override it. This change also removes the protected
DataInput.readGroupVInt method: subclasses should delegate or reimplement it entirely.

### OpenNLP dependency upgrade

[Apache OpenNLP](https://opennlp.apache.org) 2.x opens the door to accessing various models via the ONNX runtime.  To migrate you will need to update any deprecated OpenNLP methods that you may be using.

### Snowball dependency upgrade

Snowball has folded the "German2" stemmer into their "German" stemmer, so there's no "German2" anymore.  For Lucene APIs (TokenFilter, TokenFilterFactory) that accept String, "German2" will be mapped to "German" to avoid breaking users. If you were previously creating German2Stemmer instances, you'll need to change your code to create GermanStemmer instances instead.  For more information see https://snowballstem.org/algorithms/german2/stemmer.html

### Romanian analysis

RomanianAnalyzer now works with Romanian in its modern unicode form, and normalizes cedilla forms to forms with commas. Both forms are still in use in "the wild": you should reindex Romanian documents.

### IndexWriter requires a parent document field in order to use index sorting with document blocks (GITHUB#12829)

For indices newly created as of 10.0.0 onwards, IndexWriter preserves document blocks indexed via
IndexWriter#addDocuments or IndexWriter#updateDocuments when index sorting is configured. Document blocks are maintained
alongside their parent documents during sort and merge. The internally used parent field must be configured in
IndexWriterConfig only if index sorting is used together with documents blocks. See `IndexWriterConfig#setParendField`
for reference.

### Minor API changes in MatchHighlighter and MatchRegionRetriever. (GITHUB#12881)

The API of interfaces for accepting highlights has changed to allow performance improvements. Look at the issue and the PR diff to get
a sense of what's changed (changes are minor).

### Removed deprecated IndexSearcher.doc, IndexReader.document, IndexReader.getTermVectors (GITHUB#11998)

The deprecated Stored Fields and Term Vectors apis relied upon threadlocal storage and have been removed.

Instead, call storedFields()/termVectors() to return an instance which can fetch data for multiple documents,
and will be garbage-collected as usual.

For example:
```java
TopDocs hits = searcher.search(query, 10);
StoredFields storedFields = reader.storedFields();
for (ScoreDoc hit : hits.scoreDocs) {
    Document doc = storedFields.document(hit.doc);
}
```

Note that these StoredFields and TermVectors instances should only be consumed in the thread where
they were acquired. For instance, it is illegal to share them across threads.

### Field can no longer configure a TokenStream independently from a value

Lucene 9.x and earlier versions allowed to set a TokenStream on Field instances
independently from a string, binary or numeric value. This is no longer allowed
on the base Field class. If you need to replicate this behavior, you need to
either provide two fields, one with a TokenStream and another one with a value,
or create a sub-class of Field that overrides `TokenStream
tokenStream(Analyzer, TokenStream)` to return a custom TokenStream.

### PersianStemFilter is added to PersianAnalyzer (LUCENE-10312)

PersianAnalyzer now includes PersianStemFilter, that would change analysis results. If you need the exactly same analysis
behaviour as 9.x, clone `PersianAnalyzer` in 9.x or create custom analyzer by using `CustomAnalyzer` on your own.

### AutomatonQuery/CompiledAutomaton/RunAutomaton/RegExp no longer determinize (LUCENE-10010)

These classes no longer take a `determinizeWorkLimit` and no longer determinize
behind the scenes. It is the responsibility of the caller to call
`Operations.determinize()` for DFA execution.

### RegExp optional complement syntax has been deprecated

Support for the optional complement syntax (`~`) has been deprecated.
The `COMPLEMENT` syntax flag has been removed and replaced by the
`DEPRECATED_COMPLEMENT` flag. Users wanting to enable the deprecated
complement support can do so by explicitly passing a syntax flags that
has `DEPRECATED_COMPLEMENT` when creating a `RegExp`. For example:
`new RegExp("~(foo)", RegExp.DEPRECATED_COMPLEMENT)`.

Alternatively, and quite commonly, a more simple _complement bracket expression_,
`[^...]`, may be a suitable replacement, For example, `[^fo]` matches any
character that is not an `f` or `o`.

### DocValuesFieldExistsQuery, NormsFieldExistsQuery and KnnVectorFieldExistsQuery removed in favor of FieldExistsQuery (LUCENE-10436)

These classes have been removed and consolidated into `FieldExistsQuery`. To migrate, caller simply replace those classes
with the new one during object instantiation.

### Normalizer and stemmer classes are now package private (LUCENE-10561)

Except for a few exceptions, almost all normalizer and stemmer classes are now package private. If your code depends on
constants defined in them, copy the constant values and re-define them in your code.

### LongRangeFacetCounts / DoubleRangeFacetCounts #getTopChildren behavior change (LUCENE-10614)

The behavior of `LongRangeFacetCounts`/`DoubleRangeFacetCounts` `#getTopChildren` actually returns
the top-n ranges ordered by count from 10.0 onwards (as described in the `Facets` API) instead
of returning all ranges ordered by constructor-specified range order. The pre-existing behavior in
9.x and earlier can be retained by migrating to the new `Facets#getAllChildren` API (LUCENE-10550).

### SortedSetDocValues#NO_MORE_ORDS removed (LUCENE-10603)

`SortedSetDocValues#nextOrd()` no longer returns `NO_MORE_ORDS` when ordinals are exhausted for the
currently-positioned document. Callers should instead use `SortedSetDocValues#docValueCount()` to
determine the number of valid ordinals for the currently-positioned document up-front. It is now
illegal to call `SortedSetDocValues#nextOrd()` more than `SortedSetDocValues#docValueCount()` times
for the currently-positioned document (doing so will result in undefined behavior).

### IOContext removed from Directory#openChecksumInput (GITHUB#12027)

`Directory#openChecksumInput` no longer takes in `IOContext` as a parameter, and will always use value
`IOContext.READONCE` for opening internally, as that's the only valid usage pattern for checksum input.
Callers should remove the parameter when calling this method.


### DaciukMihovAutomatonBuilder is renamed to StringsToAutomaton and made package-private

The former `DaciukMihovAutomatonBuilder#build` functionality is exposed through `Automata#makeStringUnion`.
Users should be able to directly migrate to the `Automata` static method as a 1:1 replacement.

### Remove deprecated IndexSearcher#getExecutor (GITHUB#12580)

The deprecated getter for the `Executor` that was optionally provided to the `IndexSearcher` constructors
has been removed. Users that want to execute concurrent tasks should rely instead on the `TaskExecutor`
that the searcher holds, retrieved via `IndexSearcher#getTaskExecutor`.

### CheckIndex params -slow and -fast are deprecated, replaced by -level X (GITHUB#11023)

The `CheckIndex` former `-fast` behaviour of performing checksum checks only, is now the default.
Added a new parameter: `-level X`, to set the detail level of the index check. The higher the value, the more checks are performed.
Sample `-level` usage: `1` (Default) - Checksum checks only, `2` - all level 1 checks as well as logical integrity checks, `3` - all
level 2 checks as well as slow checks.

### Expressions module now uses `MethodHandle` and hidden classes (GITHUB#12873)

Custom functions in the expressions module must now be passed in a `Map` using `MethodHandle` as values.
To convert legacy code using maps of reflective `java.lang.reflect.Method`, use the converter method
`JavascriptCompiler#convertLegacyFunctions`. This should make the mapping mostly compatible.
The use of `MethodHandle` and [Dynamic Class-File Constants (JEP 309)](https://openjdk.org/jeps/309)
now also allows to pass private methods or methods from different classloaders. It is also possible
to adapt guards or filters using the `MethodHandles` class.

The new implementation of the Javascript expressions compiler no longer supports use of custom
`ClassLoader`, because it uses the new JDK 15 feature [hidden classes (JEP 371)](https://openjdk.org/jeps/371).
Due to the use of `MethodHandle`, classloader isolation is no longer needed, because JS code can only call
MHs that were resolved by the application before using the expressions module.

### `Expression#evaluate()` declares to throw IOException (GITHUB#12878)

The expressions module has changed the `Expression#evaluate()` method signature:
It now declares that it may throw `IOException`. This was an oversight because
compiled expressions call `DoubleValues#doubleValue` behind the scenes, which
may throw `IOException` on index problems, bubbling up unexpectedly to the caller.

### PathHierarchyTokenizer and ReversePathHierarchyTokenizer do not produce overlapping tokens

`(Reverse)PathHierarchyTokenizer` now produces sequential (instead of overlapping) tokens with accurate
offsets, making positional queries and highlighters possible for fields tokenized with this tokenizer.

### Removed Scorable#docID() (GITHUB#12407)

This method has been removed in order to enable more search-time optimizations.
Use the doc ID passed to `LeafCollector#collect` to know which doc ID is being
collected.

### ScoreCachingWrappingScorer now wraps a LeafCollector instead of a Scorable (GITHUB#12407)

In order to adapt to the removal of `Scorable#docID()`,
`ScoreCachingWrappingScorer` now wraps a `LeafCollector` rather than a
`Scorable`.

### Some classes converted to records classes (GITHUB#13207)

Some classes with only final fields and no programming logic were converted to `record` classes.
Those changes are mostly compatible with Lucene 9.x code (constructors, accessor methods), but
record's fields are only available with accessor methods. Some code may need to be refactored to
access the members using method calls instead of field accesses. Affected classes:

- `IOContext`, `MergeInfo`, and `FlushInfo` (GITHUB#13205)
- `BooleanClause` (GITHUB#13261)
- `TotalHits` (GITHUB#13762)
- `TermAndVector` (GITHUB#13772)
- Many basic Lucene classes, including `CollectionStatistics`, `TermStatistics` and `LeafMetadata` (GITHUB#13328)

### Boolean flags on IOContext replaced with a new ReadAdvice enum.

The `readOnce`, `load` and `random` flags on `IOContext` have been replaced with a new `ReadAdvice`
enum.

### IOContext.LOAD and IOContext.READ removed

`IOContext#LOAD` has been removed, it should be replaced with
`ioContext.withReadAdvice(ReadAdvice.NORMAL)`.

`IOContext.READ` has been removed, it should be replaced with `IOContext.DEFAULT`.

### TimeLimitingCollector removed (GITHUB#13243)

`TimeLimitingCollector` has been removed, use `IndexSearcher#setTimeout(QueryTimeout)` to time out queries instead.

### IndexSearch#search(Query, Collector) being deprecated in favor of IndexSearcher#search(Query, CollectorManager) (LUCENE-10002)

`IndexSearch#search(Query, Collector)` is now being deprecated in favor of `IndexSearcher#search(Query, CollectorManager)`,
as `CollectorManager` implementation would allow taking advantage of intra-query concurrency via its map-reduce API design.
To migrate, use a provided `CollectorManager` implementation that suits your use cases, or change your `Collector` implementation
to follow the new API pattern. The straight forward approach would be to instantiate the single-threaded `Collector` in a wrapper `CollectorManager`.

For example
```java
public class CustomCollectorManager implements CollectorManager<CustomCollector, List<Object>> {
    @Override
    public CustomCollector newCollector() throws IOException {
        return new CustomCollector();
    }

    @Override
    public List<Object> reduce(Collection<CustomCollector> collectors) throws IOException {
        List<Object> all = new ArrayList<>();
        for (CustomCollector c : collectors) {
            all.addAll(c.getResult());
        }

        return all;
    }
}

List<Object> results = searcher.search(query, new CustomCollectorManager());
```

### Accountable interface removed from KnnVectorsReader (GITHUB#13255)

`KnnVectorsReader` objects use small heap memory, so it's not worth maintaining heap usage for them hence removed
`Accountable` interface from `KnnVectorsReader`.

### Deprecated code removal (GITHUB#13262)

1. `IntField(String name, int value)`. Use `IntField(String, int, Field.Store)` with `Field.Store#NO` instead.
2. `DoubleField(String name, double value)`. Use `DoubleField(String, double, Field.Store)` with `Field.Store#NO` instead.
2. `FloatField(String name, float value)`. Use `FloatField(String, float, Field.Store)` with `Field.Store#NO` instead.
3. `LongField(String name, long value)`. Use `LongField(String, long, Field.Store)` with `Field.Store#NO` instead.
4. `LongPoint#newDistanceFeatureQuery(String field, float weight, long origin, long pivotDistance)`. Use `LongField#newDistanceFeatureQuery` instead
5. `BooleanQuery#TooManyClauses`, `BooleanQuery#getMaxClauseCount()`, `BooleanQuery#setMaxClauseCount()`. Use `IndexSearcher#TooManyClauses`, `IndexSearcher#getMaxClauseCount()`, `IndexSearcher#setMaxClauseCount()` instead
6. `ByteBuffersDataInput#size()`. Use `ByteBuffersDataInput#length()` instead
7. `SortedSetDocValuesFacetField#label`. `FacetsConfig#pathToString(String[])` can be applied to path as a replacement if string path is desired.

### Auto I/O throttling disabled by default in ConcurrentMergeScheduler (GITHUB#13293)

ConcurrentMergeScheduler now disables auto I/O throttling by default. There is still some throttling
happening at the CPU level, since ConcurrentMergeScheduler has a maximum number of threads it can
use, which is only a fraction of the total number of threads of the host by default.

### FieldInfos#hasVectors and FieldInfo#hasVectors renamed to hasTermVectors

To reduce confusion between term vectors and numeric vectors, `hasVectors` has been renamed to
`hasTermVectors`.

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
Performance gains have been observed with these encoding changes. These changes were introduced
in 9.0, and 9.x releases remain backwards-compatible with 8.x indexes, but starting with 10.0,
only the newer formats are supported. Users will need to create a new index with all their
documents using 9.0 or later to pick up the new format and remain compatible with 10.x releases.
Just re-adding documents to an existing index is not enough to pick up the changes as the
format will "stick" to whatever version was used to initially create the index.

Additionally, `OrdinalsReader` (and sub-classes) are fully removed starting with 10.0. These
classes were `@Deprecated` starting with 9.0. Users are encouraged to rely on the default
taxonomy facet encodings where possible. If custom formats are needed, users will need
to manage the indexed data on their own and create new `Facet` implementations to use it.

### `Weight#scorerSupplier` is declared abstract, and `Weight#scorer` methd is marked final

The `Weight#scorerSupplier` method is now declared abstract, compelling child classes to implement the ScorerSupplier
interface. Additionally, `Weight#scorer` is now declared final, with its implementation being delegated to
`Weight#scorerSupplier` for the scorer.

### Reference to `weight` is removed from Scorer (GITHUB#13410)

The `weight` has been removed from the Scorer class. Consequently, the constructor, `Scorer(Weight)`,and a getter,
`Scorer#getWeight`, has also been eliminated. References to weight have also been removed from nearly all the subclasses
of Scorer, including ConstantScoreScorer, TermScorer, and others.

Additionally, several APIs have been modified to remove the weight reference, as it is no longer necessary.
Specifically, the method `FunctionValues#getScorer(Weight weight, LeafReaderContext readerContext)` has been updated to
`FunctionValues#getScorer(LeafReaderContext readerContext)`.

Callers must now keep track of the Weight instance that created the Scorer if they need it, instead of relying on
Scorer.

### `FacetsCollector#search` utility methods moved and updated

The static `search` methods exposed by `FacetsCollector` have been moved to `FacetsCollectorManager`.
Furthermore, they take a `FacetsCollectorManager` last argument in place of a `Collector` so that they support
intra query concurrency. The return type has also be updated to `FacetsCollectorManager.FacetsResult` which includes
both `TopDocs` as well as facets results included in a reduced `FacetsCollector` instance.

### `SearchWithCollectorTask` no longer supports the `collector.class` config parameter

`collector.class` used to allow users to load a custom collector implementation. `collector.manager.class`
replaces it by allowing users to load a custom collector manager instead.

### BulkScorer#score(LeafCollector collector, Bits acceptDocs) removed

Use `BulkScorer#score(LeafCollector collector, Bits acceptDocs, int min, int max)` instead. In order to score the
entire leaf, provide `0` as min and `DocIdSetIterator.NO_MORE_DOCS` as max. `BulkScorer` subclasses that override
such method need to instead override the method variant that takes the range of doc ids as well as arguments.

### CollectorManager#newCollector and Collector#getLeafCollector contract

With the introduction of intra-segment query concurrency support, multiple `LeafCollector`s may be requested for the
same `LeafReaderContext` via `Collector#getLeafCollector(LeafReaderContext)` across the different `Collector` instances
returned by multiple `CollectorManager#newCollector` calls. Any logic or computation that needs to happen
once per segment requires specific handling in the collector manager implementation. See `TotalHitCountCollectorManager`
as an example. Individual collectors don't need to be adapted as a specific `Collector` instance will still see a given
`LeafReaderContext` once, given that it is not possible to add more than one partition of the same segment to the same
leaf slice.

### Weight#scorer, Weight#bulkScorer and Weight#scorerSupplier contract

With the introduction of intra-segment query concurrency support, multiple `Scorer`s, `ScorerSupplier`s or `BulkScorer`s
may be requested for the same `LeafReaderContext` instance as part of a single search call. That may happen concurrently
from separate threads each searching a specific doc id range of the segment. `Weight` implementations that rely on the
assumption that a scorer, bulk scorer or scorer supplier for a given `LeafReaderContext` is requested once per search
need updating.

### Signature of IndexSearcher#searchLeaf changed

With the introduction of intra-segment query concurrency support, the `IndexSearcher#searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector)`
method now accepts two additional int arguments to identify the min/max range of doc ids that will be searched in this
leaf partition`: IndexSearcher#searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)`.
Subclasses of `IndexSearcher` that call or override the `searchLeaf` method need to be updated accordingly.

### Signature of static IndexSearch#slices method changed

The static `IndexSearcher#slices(List<LeafReaderContext> leaves, int maxDocsPerSlice, int maxSegmentsPerSlice)`
method now supports an additional 4th and last argument to optionally enable creating segment partitions:
`IndexSearcher#slices(List<LeafReaderContext> leaves, int maxDocsPerSlice, int maxSegmentsPerSlice, boolean allowSegmentPartitions)`

### TotalHitCountCollectorManager constructor

`TotalHitCountCollectorManager` now requires that an array of `LeafSlice`s, retrieved via `IndexSearcher#getSlices`,
is provided to its constructor. Depending on whether segment partitions are present among slices, the manager can
optimize the type of collectors it creates and exposes via `newCollector`.

### `IndexSearcher#search(List<LeafReaderContext>, Weight, Collector)` removed

The protected `IndexSearcher#search(List<LeafReaderContext> leaves, Weight weight, Collector collector)` method has been
removed in favour of the newly introduced `search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector)`.
`IndexSearcher` subclasses that override this method need to instead override the new method.

### Indexing vectors with 8 bit scalar quantization is no longer supported but 7 and 4 bit quantization still work (GITHUB#13519)

8 bit scalar vector quantization is no longer supported: it was buggy
starting in 9.11 (GITHUB#13197).  4 and 7 bit quantization are still
supported.  Existing (9.11) Lucene indices that previously used 8 bit
quantization can still be read/searched but the results from
`KNN*VectorQuery` are silently buggy.  Further 8 bit quantized vector
indexing into such (9.11) indices is not permitted, so your path
forward if you wish to continue using the same 9.11 index is to index
additional vectors into the same field with either 4 or 7 bit
quantization (or no quantization), and ensure all older (9.x written)
segments are rewritten either via `IndexWriter.forceMerge` or
`IndexWriter.addIndexes(CodecReader...)`, or reindexing entirely.

### Vector values APIs switched to primarily random-access

`{Byte/Float}VectorValues` no longer inherit from `DocIdSetIterator`. Rather they extend a common class, `KnnVectorValues`, that provides a random access API (previously provided by `RandomAccessVectorValues`, now removed), and an `iterator()` method for retrieving `DocIndexIterator`: an iterator which is a DISI that also provides an `index()` method. Therefore, any iteration over vector values must now be performed using the values' `iterator()`. Random access works as before, but does not require casting to `RandomAccessVectorValues`.
