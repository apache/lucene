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

/**
 * This package contains classes that implement {@linkplain
 * org.apache.lucene.queries.intervals.Intervals interval function} support for the {@linkplain
 * org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser standard syntax
 * parser}.
 *
 * <h2>What are interval functions?</h2>
 *
 * <p>Interval functions are a powerful tool to express search needs in terms of one or more
 * contiguous fragments of text and their relationship to one another. Interval functions are
 * implemented by an {@linkplain org.apache.lucene.queries.intervals.IntervalQuery IntervalQuery}
 * but many ready-to-use factory methods are provided in the {@linkplain
 * org.apache.lucene.queries.intervals.Intervals Intervals} class.
 *
 * <p>When Lucene indexes documents (or rather: document fields) the input text is typically split
 * into <em>tokens</em>. The details of how this tokenization is performed depends on how the
 * field's {@link org.apache.lucene.analysis.Analyzer} is set up. In the end, each token would
 * typically have an associated <em>position</em> in the token stream. For example, the following
 * sentence:
 *
 * <p class="example sentence with-highlights">The quick brown fox jumps over the lazy dog
 *
 * <p>could be transformed into the following token stream (note some token positions are "blank"
 * (grayed out) &mdash; these positions reflect stop words that are typically not indexed at all).
 *
 * <p class="example sentence with-highlights with-positions"><span style="color:
 * lightgrey">The</span><sub>&mdash;</sub> quick<sub>2</sub> brown<sub>3</sub> fox<sub>4</sub>
 * jumps<sub>5</sub> over<sub>6</sub> <span style="color: lightgrey">the</span><sub>&mdash;</sub>
 * lazy<sub>7</sub> dog<sub>8</sub>
 *
 * <p>Remembering that intervals are contiguous spans between two positions in a document, consider
 * the following example interval function query: <code>fn:ordered(brown dog)</code>. This query
 * selects any span of text between terms <code>brown</code> and <code>dog</code>. In our example,
 * this would correspond to the highlighted fragment below.
 *
 * <p class="example sentence with-highlights">The quick <span class="highlight">brown fox jumps
 * over the lazy dog</span>
 *
 * <p>This type of interval function can be called an interval <em>selector</em>. The second class
 * of interval functions works by combining or filtering other intervals depending on certain
 * criteria.
 *
 * <p>The matching interval in the above example can be of any length &mdash; if the word <code>
 * brown</code> occurs at the beginning of the document and the word <code>dog</code> at the very
 * end of the document, the interval would be very long (it would cover the entire document!). Let's
 * say we want to restrict the matches to only those intervals with at most 3 positions between the
 * search terms: <code>fn:maxgaps(3 fn:ordered(brown dog))</code>.
 *
 * <p>There are five tokens in between search terms (so five "gaps" between the matching interval's
 * positions) and the above query no longer matches our example document at all.
 *
 * <p>Interval filtering functions allow expressing a variety of conditions other Lucene queries
 * cannot. For example, consider this interval query that searches for words <code>lazy</code> or
 * <code>quick</code> but only if they are in the neighborhood of one position from any of the words
 * <code>dog</code> or <code>fox</code>:
 *
 * <p><code>fn:within(fn:or(lazy quick) 1 fn:or(dog fox))</code>
 *
 * <p>The result of this query is correctly shown below (only the word <code>lazy</code> matches the
 * query, <code>quick</code> is 2 positions away from <code>fox</code>).
 *
 * <p class="example sentence with-highlights">The quick brown fox jumps over the <span
 * class="highlight">lazy</span> dog
 *
 * <p>The remaining part of this document provides more information on the available functions and
 * their expected behavior.
 *
 * <h2>Classification of interval functions</h2>
 *
 * <p>The following groups of interval functions are available in the {@link
 * org.apache.lucene.queryparser.flexible.standard.StandardQueryParser}.
 *
 * <table class="table" style="width: auto">
 *   <caption>Interval functions grouped by similar functionality.</caption>
 *   <thead>
 *     <tr>
 *       <th>Terms</th>
 *       <th>Alternatives</th>
 *       <th>Length</th>
 *       <th>Context</th>
 *       <th>Ordering</th>
 *       <th>Containment</th>
 *     </tr>
 *   </thead>
 *
 *   <tbody>
 *   <tr>
 *     <td>
 *       <em>term literals</em><br>
 *       <code>fn:wildcard</code><br>
 *     </td>
 *     <td>
 *       <code>fn:or</code><br>
 *       <code>fn:atLeast</code>
 *     </td>
 *     <td>
 *       <code>fn:maxgaps</code><br>
 *       <code>fn:maxwidth</code>
 *     </td>
 *     <td>
 *       <code>fn:before</code><br>
 *       <code>fn:after</code><br>
 *       <code>fn:extend</code><br>
 *       <code>fn:within</code><br>
 *       <code>fn:notWithin</code>
 *     </td>
 *     <td>
 *       <code>fn:ordered</code><br>
 *       <code>fn:unordered</code><br>
 *       <code>fn:phrase</code><br>
 *       <code>fn:unorderedNoOverlaps</code>
 *     </td>
 *     <td>
 *       <code>fn:containedBy</code><br>
 *       <code>fn:notContainedBy</code><br>
 *       <code>fn:containing</code><br>
 *       <code>fn:notContaining</code><br>
 *       <code>fn:overlapping</code><br>
 *       <code>fn:nonOverlapping</code>
 *     </td>
 *   </tr>
 *   </tbody>
 * </table>
 *
 * <p>All examples in the description of interval functions (below) assume a document with the
 * following content:
 *
 * <p class="example sentence with-highlights">The quick brown fox jumps over the lazy dog
 *
 * <h3><em>term literals</em></h3>
 *
 * <p>Quoted or unquoted character sequences are converted into (analyzed) text intervals. While a
 * single term typically results in a single-term interval, a quoted multi-term phrase will produce
 * an interval matching the corresponding sequence of tokens. Note this is different from the <code>
 * fn:phrase</code> function which takes a sequence of sub-intervals.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:or(quick "fox")</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick</span> brown <span class="highlight">fox</span> jumps over
 *             the lazy dog
 *         <li><code>fn:or(\"quick fox\")</code> (<em>The document would not match &mdash; no phrase
 *             <code>quick fox</code> exists.</em>)
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy dog
 *         <li><code>fn:phrase(quick brown fox)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:wildcard</h3>
 *
 * <p>Matches the disjunction of all terms that match a wildcard glob.
 *
 * <p><em>Important!</em> The expanded wildcard must not match more than 128 terms. This is an
 * internal limitation that prevents blowing up memory on, for example, prefix expansions that would
 * cover huge numbers of alternatives.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:wildcard(glob)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>glob</code>
 *         <dd>term glob to expand (based on the contents of the index).
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:wildcard(jump*)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox <span
 *             class="highlight">jumps</span> over the lazy dog
 *         <li><code>fn:wildcard(br*n)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown</span> fox jumps over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:or</h3>
 *
 * <p>Matches the disjunction of nested intervals.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:or(sources...)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>sources</code>
 *         <dd>sub-intervals (terms or other functions)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:or(dog fox)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy <span class="highlight">dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:atLeast</h3>
 *
 * <p>Matches documents that contain at least the provided number of source intervals.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:atLeast(min sources...)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>min</code>
 *         <dd>an integer specifying minimum number of sub-interval arguments that must match.
 *         <dt><code>sources</code>
 *         <dd>sub-intervals (terms or other functions)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:atLeast(2 quick fox "furry dog")</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the lazy dog
 *         <li><code>fn:atLeast(2 fn:unordered(furry dog) fn:unordered(brown dog) lazy quick)</code>
 *             <em>(This query results in multiple overlapping intervals.)</em>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox jumps over the lazy</span> dog<br>
 *             The <span class="highlight">quick brown fox jumps over the lazy dog</span><br>
 *             The quick <span class="highlight">brown fox jumps over the lazy dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:maxgaps</h3>
 *
 * <p>Accepts <code>source</code> interval if it has at most <code>max</code> position gaps.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:maxgaps(gaps source)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>gaps</code>
 *         <dd>an integer specifying maximum number of source's position gaps.
 *         <dt><code>source</code>
 *         <dd>source sub-interval.
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:maxgaps(0 fn:ordered(fn:or(quick lazy) fn:or(fox dog)))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the <span class="highlight">lazy dog</span>
 *         <li><code>fn:maxgaps(1 fn:ordered(fn:or(quick lazy) fn:or(fox dog)))</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the <span class="highlight">lazy
 *             dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:maxwidth</h3>
 *
 * <p>Accepts <code>source</code> interval if it has at most the given width (position span).
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:maxwidth(max source)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>max</code>
 *         <dd>an integer specifying maximum width of source's position span.
 *         <dt><code>source</code>
 *         <dd>source sub-interval.
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:maxwidth(2 fn:ordered(fn:or(quick lazy) fn:or(fox dog)))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the <span class="highlight">lazy dog</span>
 *         <li><code>fn:maxwidth(3 fn:ordered(fn:or(quick lazy) fn:or(fox dog)))</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the <span class="highlight">lazy
 *             dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:phrase</h3>
 *
 * <p>Matches an ordered, gapless sequence of source intervals.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:phrase(sources...)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>sources</code>
 *         <dd>sub-intervals (terms or other functions)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:phrase(quick brown fox)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the lazy dog
 *         <li><code>fn:phrase(fn:ordered(quick fox) jumps)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox jumps</span> over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:ordered</h3>
 *
 * <p>Matches an ordered span containing all source intervals, possibly with gaps in between their
 * respective source interval positions. Source intervals must not overlap.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:ordered(sources...)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>sources</code>
 *         <dd>sub-intervals (terms or other functions)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:ordered(quick jumps dog)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox jumps over the lazy dog</span>
 *         <li><code>fn:ordered(quick fn:or(fox dog))</code> <em>(Note only the shorter match out of
 *             the two alternatives is included in the result; the algorithm is not required to
 *             return or highlight all matching interval alternatives).</em>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the lazy dog
 *         <li><code>fn:ordered(quick jumps fn:or(fox dog))</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox jumps over the lazy dog</span>
 *         <li><code>fn:ordered(fn:phrase(brown fox) fn:phrase(fox jumps))</code> <em>(Sources
 *             overlap, no matches.)</em>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:unordered</h3>
 *
 * <p>Matches an unordered span containing all source intervals, possibly with gaps in between their
 * respective source interval positions. Source intervals may overlap.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:unordered(sources...)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>sources</code>
 *         <dd>sub-intervals (terms or other functions)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:unordered(dog jumps quick)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox jumps over the lazy dog</span>
 *         <li><code>fn:unordered(fn:or(fox dog) quick)</code> <em>(Note only the shorter match out
 *             of the two alternatives is included in the result; the algorithm is not required to
 *             return or highlight all matching interval alternatives).</em>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over the lazy dog
 *         <li><code>fn:unordered(fn:phrase(brown fox) fn:phrase(fox jumps))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown fox jumps</span> over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:unorderedNoOverlaps</h3>
 *
 * <p>Matches an unordered span containing two source intervals, possibly with gaps in between their
 * respective source interval positions. Source intervals must not overlap.
 *
 * <p>Note that, unlike <code>fn:unordered</code>, this function takes a fixed number of arguments
 * (two).
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:unorderedNoOverlaps(source1 source2)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source1</code>
 *         <dd>sub-interval (term or other function)
 *         <dt><code>source2</code>
 *         <dd>sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:unorderedNoOverlaps(fn:phrase(fox jumps) brown)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown fox jumps</span> over the lazy dog
 *         <li><code>fn:unorderedNoOverlaps(fn:phrase(brown fox) fn:phrase(fox jumps))</code>
 *             <em>(Sources overlap, no matches.)</em>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:before</h3>
 *
 * <p>Matches intervals from the source that appear before intervals from the reference.
 *
 * <p>Reference intervals will not be part of the match (this is a filtering function).
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:before(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:before(fn:or(brown lazy) fox)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown</span> fox jumps over the lazy dog
 *         <li><code>fn:before(fn:or(brown lazy) fn:or(dog fox))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown</span> fox jumps over the <span class="highlight">lazy</span>
 *             dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:after</h3>
 *
 * <p>Matches intervals from the source that appear after intervals from the reference.
 *
 * <p>Reference intervals will not be part of the match (this is a filtering function).
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:after(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:after(fn:or(brown lazy) fox)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the <span class="highlight">lazy</span> dog
 *         <li><code>fn:after(fn:or(brown lazy) fn:or(dog fox))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the <span class="highlight">lazy</span> dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:extend</h3>
 *
 * <p>Matches an interval around another source, extending its span by a number of positions before
 * and after.
 *
 * <p>This is an advanced function that allows extending the left and right "context" of another
 * interval.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:extend(source before after)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>before</code>
 *         <dd>an integer number of positions to extend to the left of the source
 *         <dt><code>after</code>
 *         <dd>an integer number of positions to extend to the right of the source
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:extend(fox 1 2)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown fox jumps over</span> the lazy dog
 *         <li><code>fn:extend(fn:or(dog fox) 2 0)</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over <span class="highlight">the lazy
 *             dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:within</h3>
 *
 * <p>Matches intervals of the source that appear within the provided number of positions from the
 * intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:within(source positions reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>positions</code>
 *         <dd>an integer number of maximum positions between source and reference
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:within(fn:or(fox dog) 1 fn:or(quick lazy))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy <span class="highlight">dog</span>
 *         <li><code>fn:within(fn:or(fox dog) 2 fn:or(quick lazy))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy <span class="highlight">dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:notWithin</h3>
 *
 * <p>Matches intervals of the source that do <em>not</em> appear within the provided number of
 * positions from the intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:notWithin(source positions reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>positions</code>
 *         <dd>an integer number of maximum positions between source and reference
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:notWithin(fn:or(fox dog) 1 fn:or(quick lazy))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:containedBy</h3>
 *
 * <p>Matches intervals of the source that are contained by intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:containedBy(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:containedBy(fn:or(fox dog) fn:ordered(quick lazy))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy dog
 *         <li><code>fn:containedBy(fn:or(fox dog) fn:extend(lazy 3 3))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy <span class="highlight">dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:notContainedBy</h3>
 *
 * <p>Matches intervals of the source that are <em>not</em> contained by intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:notContainedBy(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:notContainedBy(fn:or(fox dog) fn:ordered(quick lazy))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy <span class="highlight">dog</span>
 *         <li><code>fn:notContainedBy(fn:or(fox dog) fn:extend(lazy 3 3))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy dog
 *       </ul>
 * </dl>
 *
 * <h3>fn:containing</h3>
 *
 * <p>Matches intervals of the source that contain at least one intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:containing(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:containing(fn:extend(fn:or(lazy brown) 1 1) fn:or(fox dog))</code>
 *             <p class="example sentence with-highlights left-aligned">The <span
 *             class="highlight">quick brown fox</span> jumps over <span class="highlight">the lazy
 *             dog</span>
 *         <li><code>fn:containing(fn:atLeast(2 quick fox dog) jumps)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox jumps over the lazy dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:notContaining</h3>
 *
 * <p>Matches intervals of the source that do <em>not</em> contain any intervals of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:notContaining(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:notContaining(fn:extend(fn:or(fox dog) 1 0) fn:or(brown yellow))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the <span class="highlight">lazy dog</span>
 *         <li><code>fn:notContaining(fn:ordered(fn:or(the The) fn:or(fox dog)) brown)</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over <span class="highlight">the lazy dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:overlapping</h3>
 *
 * <p>Matches intervals of the source that overlap with at least one interval of the reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:overlapping(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:overlapping(fn:phrase(brown fox) fn:phrase(fox jumps))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown fox</span> jumps over the lazy dog
 *         <li><code>fn:overlapping(fn:or(fox dog) fn:extend(lazy 2 2))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown fox jumps
 *             over the lazy <span class="highlight">dog</span>
 *       </ul>
 * </dl>
 *
 * <h3>fn:nonOverlapping</h3>
 *
 * <p>Matches intervals of the source that do <em>not</em> overlap with any intervals of the
 * reference.
 *
 * <dl class="dl-horizontal narrow">
 *   <dt>Arguments
 *   <dd>
 *       <p><code>fn:nonOverlapping(source reference)</code>
 *       <dl class="dl-horizontal narrow">
 *         <dt><code>source</code>
 *         <dd>source sub-interval (term or other function)
 *         <dt><code>reference</code>
 *         <dd>reference sub-interval (term or other function)
 *       </dl>
 *   <dt>Examples
 *   <dd>
 *       <ul>
 *         <li><code>fn:nonOverlapping(fn:phrase(brown fox) fn:phrase(lazy dog))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick <span
 *             class="highlight">brown fox</span> jumps over the lazy dog
 *         <li><code>fn:nonOverlapping(fn:or(fox dog) fn:extend(lazy 2 2))</code>
 *             <p class="example sentence with-highlights left-aligned">The quick brown <span
 *             class="highlight">fox</span> jumps over the lazy dog
 *       </ul>
 * </dl>
 */
package org.apache.lucene.queryparser.flexible.standard.nodes.intervalfn;
