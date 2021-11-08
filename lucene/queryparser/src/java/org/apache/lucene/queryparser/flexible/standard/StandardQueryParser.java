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
package org.apache.lucene.queryparser.flexible.standard;

import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TooManyListenersException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryParserHelper;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.standard.config.FuzzyConfig;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

/**
 * The {@link StandardQueryParser} is a pre-assembled query parser that supports most features of
 * the {@linkplain org.apache.lucene.queryparser.classic.QueryParser classic Lucene query parser},
 * allows dynamic configuration of some of its features (like multi-field expansion or wildcard
 * query restrictions) and adds support for new query types and expressions.
 *
 * <p>The {@link StandardSyntaxParser} is an extension of the {@link QueryParserHelper} with
 * reasonable defaults for syntax tree parsing ({@link StandardSyntaxParser}, node processor
 * pipeline ({@link StandardQueryNodeProcessorPipeline} and node tree to {@link Query} builder
 * ({@link StandardQueryTreeBuilder}).
 *
 * <p>Typical usage, including configuration tweaks:
 *
 * <pre class="prettyprint">{@code
 * StandardQueryParser qpHelper = new StandardQueryParser();
 * StandardQueryConfigHandler config =  qpHelper.getQueryConfigHandler();
 * config.setAllowLeadingWildcard(true);
 * config.setAnalyzer(new WhitespaceAnalyzer());
 * Query query = qpHelper.parse("apache AND lucene", "defaultField");
 * }</pre>
 *
 * <h2>Supported query syntax</h2>
 *
 * <p>Standard query parser borrows most of its syntax from the {@linkplain
 * org.apache.lucene.queryparser.classic classic query parser} but adds more features and
 * expressions on top of that syntax.
 *
 * <p>A <em>query</em> consists of clauses, field specifications, grouping and Boolean operators and
 * interval functions. We will discuss them in order.
 *
 * <h3>Basic clauses</h3>
 *
 * <p>A query must contain one or more clauses. A clause can be a literal term, a phrase, a wildcard
 * expression or other expression that
 *
 * <p>The following are some examples of simple one-clause queries:
 *
 * <ul>
 *   <li><code>test</code>
 *       <p>selects documents containing the word <em>test</em> (term clause).
 *   <li><code>"test equipment"</code>
 *       <p>phrase search; selects documents containing the phrase <em>test equipment</em> (phrase
 *       clause).
 *   <li><code>"test failure"~4</code>
 *       <p>proximity search; selects documents containing the words <em>test</em> and
 *       <em>failure</em> within 4 words (positions) from each other. The provided "proximity" is
 *       technically translated into "edit distance" (maximum number of atomic word-moving
 *       operations required to transform the document's phrase into the query phrase).
 *   <li><code>tes*</code>
 *       <p>prefix wildcard matching; selects documents containing words starting with <em>tes</em>,
 *       such as: <em>test</em>, <em>testing</em> or <em>testable</em>.
 *   <li><code>/.est(s|ing)/</code>
 *       <p>documents containing words matching the provided regular expression, such as
 *       <em>resting</em> or <em>nests</em>.
 *   <li><code>nest~2</code>
 *       <p>fuzzy term matching; documents containing words within 2-edits distance (2 additions,
 *       removals or replacements of a letter) from <em>nest</em>, such as <em>test</em>,
 *       <em>net</em> or <em>rests</em>.
 * </ul>
 *
 * <h3>Field specifications</h3>
 *
 * <p>Most clauses can be prefixed by a field name and a colon: the clause will then apply to that
 * field only. If the field specification is omitted, the query parser will expand the clause over
 * all fields specified by a call to {@link StandardQueryParser#setMultiFields(CharSequence[])} or
 * will use the default field provided in the call to {@link #parse(String, String)}.
 *
 * <p>The following are some examples of field-prefixed clauses:
 *
 * <ul>
 *   <li><code>title:test</code>
 *       <p>documents containing <em>test</em> in the <code>title</code> field.
 *   <li><code>title:(die OR hard)</code>
 *       <p>documents containing <em>die</em> or <em>hard</em> in the <code>title</code> field.
 * </ul>
 *
 * <h3>Boolean operators and grouping</h3>
 *
 * <p>You can combine clauses using Boolean AND, OR and NOT operators to form more complex
 * expressions, for example:
 *
 * <ul>
 *   <li><code>test AND results</code>
 *       <p>selects documents containing both the word <em>test</em> and the word <em>results</em>.
 *   <li><code>test OR suite OR results</code>
 *       <p>selects documents with at least one of <em>test</em>, <em>suite</em> or
 *       <em>results</em>.
 *   <li><code>title:test AND NOT title:complete</code>
 *       <p>selects documents containing <em>test</em> and not containing <em>complete</em> in the
 *       <code>title</code> field.
 *   <li><code>title:test AND (pass* OR fail*)</code>
 *       <p>grouping; use parentheses to specify the precedence of terms in a Boolean clause. Query
 *       will match documents containing <em>test</em> in the <code>title</code> field and a word
 *       starting with <em>pass</em> or <em>fail</em> in the default search fields.
 *   <li><code>title:(pass fail skip)</code>
 *       <p>shorthand notation; documents containing at least one of <em>pass</em>, <em>fail</em> or
 *       <em>skip</em> in the <code>title</code> field.
 *   <li><code>title:(+test +"result unknown")</code>
 *       <p>shorthand notation; documents containing both <em>pass</em> and <em>result unknown</em>
 *       in the <code>title</code> field.
 * </ul>
 *
 * <p>Note the Boolean operators must be written in all caps, otherwise they are parsed as regular
 * terms.
 *
 * <h3>Range operators</h3>
 *
 * <p>To search for ranges of textual or numeric values, use square or curly brackets, for example:
 *
 * <ul>
 *   <li><code>name:[Jones TO Smith]</code>
 *       <p>inclusive range; selects documents whose <code>name
 *       </code> field has any value between <em>Jones</em> and <em>Smith</em>, including
 *       boundaries.
 *   <li><code>score:{2.5 TO 7.3}</code>
 *       <p>exclusive range; selects documents whose <code>score</code> field is between 2.5 and
 *       7.3, excluding boundaries.
 *   <li><code>score:{2.5 TO *]</code>
 *       <p>one-sided range; selects documents whose <code>score</code> field is larger than 2.5.
 * </ul>
 *
 * <h3>Term boosting</h3>
 *
 * <p>Terms, quoted terms, term range expressions and grouped clauses can have a floating-point
 * weight <em>boost</em> applied to them to increase their score relative to other clauses. For
 * example:
 *
 * <ul>
 *   <li><code>jones^2 OR smith^0.5</code>
 *       <p>prioritize documents with <code>jones</code> term over matches on the <code>smith</code>
 *       term.
 *   <li><code>field:(a OR b NOT c)^2.5 OR field:d</code>
 *       <p>apply the boost to a sub-query.
 * </ul>
 *
 * <h3>Special character escaping</h3>
 *
 * <p>Most search terms can be put in double quotes making special-character escaping not necessary.
 * If the search term contains the quote character (or cannot be quoted for some reason), any
 * character can be quoted with a backslash. For example:
 *
 * <ul>
 *   <li><code>\:\(quoted\+term\)\:</code>
 *       <p>a single search term <code>(quoted+term):</code> with escape sequences. An alternative
 *       quoted form would be simpler: <code>":(quoted+term):"
 *       </code>.
 * </ul>
 *
 * <h3>Minimum-should-match constraint for Boolean disjunction groups</h3>
 *
 * <p>A minimum-should-match operator can be applied to a disjunction Boolean query (a query with
 * only "OR"-subclauses) and forces the query to match documents with at least the provided number
 * of these subclauses. For example:
 *
 * <ul>
 *   <li><code>(blue crab fish)@2</code>
 *       <p>matches all documents with at least two terms from the set [blue, crab, fish] (in any
 *       order).
 *   <li><code>((yellow OR blue) crab fish)@2</code>
 *       <p>sub-clauses of a Boolean query can themselves be complex queries; here the
 *       min-should-match selects documents that match at least two of the provided three
 *       sub-clauses.
 * </ul>
 *
 * <h3>Interval function clauses</h3>
 *
 * <p>Interval functions are a powerful tool to express search needs in terms of one or more *
 * contiguous fragments of text and their relationship to one another. All interval clauses start
 * with the {@code fn:} prefix (possibly prefixed by a field specification). For example:
 *
 * <ul>
 *   <li><code>fn:ordered(quick brown fox)</code>
 *       <p>matches all documents (in the default field or in multi-field expansion) with at least
 *       one ordered sequence of <code>quick</code>, <code>
 *       brown</code> and <code>fox</code> terms.
 *   <li><code>title:fn:maxwidth(5 fn:atLeast(2 quick brown fox))</code>
 *       <p>matches all documents in the <code>title
 *       </code> field where at least two of the three terms (<code>quick</code>, <code>
 *        brown</code> and <code>fox</code>) occur within five positions of each other.
 * </ul>
 *
 * Please refer to the {@linkplain org.apache.lucene.queryparser.flexible.standard.nodes.intervalfn
 * interval functions package} for more information on which functions are available and how they
 * work.
 *
 * @see StandardQueryParser
 * @see StandardQueryConfigHandler
 * @see StandardSyntaxParser
 * @see StandardQueryNodeProcessorPipeline
 * @see StandardQueryTreeBuilder
 */
public class StandardQueryParser extends QueryParserHelper
    implements CommonQueryParserConfiguration {

  /** Constructs a {@link StandardQueryParser} object. */
  public StandardQueryParser() {
    super(
        new StandardQueryConfigHandler(),
        new StandardSyntaxParser(),
        new StandardQueryNodeProcessorPipeline(null),
        new StandardQueryTreeBuilder());
    setEnablePositionIncrements(true);
  }

  /**
   * Constructs a {@link StandardQueryParser} object and sets an {@link Analyzer} to it. The same
   * as:
   *
   * <pre class="prettyprint">
   * StandardQueryParser qp = new StandardQueryParser();
   * qp.getQueryConfigHandler().setAnalyzer(analyzer);
   * </pre>
   *
   * @param analyzer the analyzer to be used by this query parser helper
   */
  public StandardQueryParser(Analyzer analyzer) {
    this();

    this.setAnalyzer(analyzer);
  }

  @Override
  public String toString() {
    return "<StandardQueryParser config=\"" + this.getQueryConfigHandler() + "\"/>";
  }

  /**
   * Overrides {@link QueryParserHelper#parse(String, String)} so it casts the return object to
   * {@link Query}. For more reference about this method, check {@link
   * QueryParserHelper#parse(String, String)}.
   *
   * @param query the query string
   * @param defaultField the default field used by the text parser
   * @return the object built from the query
   * @throws QueryNodeException if something wrong happens along the three phases
   */
  @Override
  public Query parse(String query, String defaultField) throws QueryNodeException {

    return (Query) super.parse(query, defaultField);
  }

  /**
   * Gets implicit operator setting, which will be either {@link Operator#AND} or {@link
   * Operator#OR}.
   */
  public StandardQueryConfigHandler.Operator getDefaultOperator() {
    return getQueryConfigHandler().get(ConfigurationKeys.DEFAULT_OPERATOR);
  }

  /**
   * Sets the boolean operator of the QueryParser. In default mode ( {@link Operator#OR}) terms
   * without any modifiers are considered optional: for example <code>capital of Hungary</code> is
   * equal to <code>capital OR of OR Hungary</code>.<br>
   * In {@link Operator#AND} mode terms are considered to be in conjunction: the above mentioned
   * query is parsed as <code>capital AND of AND Hungary</code>
   */
  public void setDefaultOperator(StandardQueryConfigHandler.Operator operator) {
    getQueryConfigHandler().set(ConfigurationKeys.DEFAULT_OPERATOR, operator);
  }

  /**
   * Set to <code>true</code> to allow leading wildcard characters.
   *
   * <p>When set, <code>*</code> or <code>?</code> are allowed as the first character of a
   * PrefixQuery and WildcardQuery. Note that this can produce very slow queries on big indexes.
   *
   * <p>Default: false.
   */
  @Override
  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    getQueryConfigHandler().set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, allowLeadingWildcard);
  }

  /**
   * Set to <code>true</code> to enable position increments in result query.
   *
   * <p>When set, result phrase and multi-phrase queries will be aware of position increments.
   * Useful when e.g. a StopFilter increases the position increment of the token that follows an
   * omitted token.
   *
   * <p>Default: false.
   */
  @Override
  public void setEnablePositionIncrements(boolean enabled) {
    getQueryConfigHandler().set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, enabled);
  }

  /** @see #setEnablePositionIncrements(boolean) */
  @Override
  public boolean getEnablePositionIncrements() {
    Boolean enablePositionsIncrements =
        getQueryConfigHandler().get(ConfigurationKeys.ENABLE_POSITION_INCREMENTS);

    if (enablePositionsIncrements == null) {
      return false;

    } else {
      return enablePositionsIncrements;
    }
  }

  /**
   * By default, it uses {@link MultiTermQuery#CONSTANT_SCORE_REWRITE} when creating a prefix,
   * wildcard and range queries. This implementation is generally preferable because it a) Runs
   * faster b) Does not have the scarcity of terms unduly influence score c) avoids any {@link
   * TooManyListenersException} exception. However, if your application really needs to use the
   * old-fashioned boolean queries expansion rewriting and the above points are not relevant then
   * use this change the rewrite method.
   */
  @Override
  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    getQueryConfigHandler().set(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD, method);
  }

  /** @see #setMultiTermRewriteMethod(org.apache.lucene.search.MultiTermQuery.RewriteMethod) */
  @Override
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return getQueryConfigHandler().get(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD);
  }

  /**
   * Set the fields a query should be expanded to when the field is <code>null</code>
   *
   * @param fields the fields used to expand the query
   */
  public void setMultiFields(CharSequence[] fields) {

    if (fields == null) {
      fields = new CharSequence[0];
    }

    getQueryConfigHandler().set(ConfigurationKeys.MULTI_FIELDS, fields);
  }

  /**
   * Returns the fields used to expand the query when the field for a certain query is <code>null
   * </code>
   *
   * @return the fields used to expand the query
   */
  public CharSequence[] getMultiFields() {
    return getQueryConfigHandler().get(ConfigurationKeys.MULTI_FIELDS);
  }

  /**
   * Set the prefix length for fuzzy queries. Default is 0.
   *
   * @param fuzzyPrefixLength The fuzzyPrefixLength to set.
   */
  @Override
  public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
    QueryConfigHandler config = getQueryConfigHandler();
    FuzzyConfig fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG);

    if (fuzzyConfig == null) {
      fuzzyConfig = new FuzzyConfig();
      config.set(ConfigurationKeys.FUZZY_CONFIG, fuzzyConfig);
    }

    fuzzyConfig.setPrefixLength(fuzzyPrefixLength);
  }

  public void setPointsConfigMap(Map<String, PointsConfig> pointsConfigMap) {
    getQueryConfigHandler().set(ConfigurationKeys.POINTS_CONFIG_MAP, pointsConfigMap);
  }

  public Map<String, PointsConfig> getPointsConfigMap() {
    return getQueryConfigHandler().get(ConfigurationKeys.POINTS_CONFIG_MAP);
  }

  /** Set locale used by date range parsing. */
  @Override
  public void setLocale(Locale locale) {
    getQueryConfigHandler().set(ConfigurationKeys.LOCALE, locale);
  }

  /** Returns current locale, allowing access by subclasses. */
  @Override
  public Locale getLocale() {
    return getQueryConfigHandler().get(ConfigurationKeys.LOCALE);
  }

  @Override
  public void setTimeZone(TimeZone timeZone) {
    getQueryConfigHandler().set(ConfigurationKeys.TIMEZONE, timeZone);
  }

  @Override
  public TimeZone getTimeZone() {
    return getQueryConfigHandler().get(ConfigurationKeys.TIMEZONE);
  }

  /**
   * Sets the default slop for phrases. If zero, then exact phrase matches are required. Default
   * value is zero.
   */
  @Override
  public void setPhraseSlop(int defaultPhraseSlop) {
    getQueryConfigHandler().set(ConfigurationKeys.PHRASE_SLOP, defaultPhraseSlop);
  }

  public void setAnalyzer(Analyzer analyzer) {
    getQueryConfigHandler().set(ConfigurationKeys.ANALYZER, analyzer);
  }

  @Override
  public Analyzer getAnalyzer() {
    return getQueryConfigHandler().get(ConfigurationKeys.ANALYZER);
  }

  /** @see #setAllowLeadingWildcard(boolean) */
  @Override
  public boolean getAllowLeadingWildcard() {
    Boolean allowLeadingWildcard =
        getQueryConfigHandler().get(ConfigurationKeys.ALLOW_LEADING_WILDCARD);

    if (allowLeadingWildcard == null) {
      return false;

    } else {
      return allowLeadingWildcard;
    }
  }

  /** Get the minimal similarity for fuzzy queries. */
  @Override
  public float getFuzzyMinSim() {
    FuzzyConfig fuzzyConfig = getQueryConfigHandler().get(ConfigurationKeys.FUZZY_CONFIG);

    if (fuzzyConfig == null) {
      return FuzzyQuery.defaultMaxEdits;
    } else {
      return fuzzyConfig.getMinSimilarity();
    }
  }

  /**
   * Get the prefix length for fuzzy queries.
   *
   * @return Returns the fuzzyPrefixLength.
   */
  @Override
  public int getFuzzyPrefixLength() {
    FuzzyConfig fuzzyConfig = getQueryConfigHandler().get(ConfigurationKeys.FUZZY_CONFIG);

    if (fuzzyConfig == null) {
      return FuzzyQuery.defaultPrefixLength;
    } else {
      return fuzzyConfig.getPrefixLength();
    }
  }

  /** Gets the default slop for phrases. */
  @Override
  public int getPhraseSlop() {
    Integer phraseSlop = getQueryConfigHandler().get(ConfigurationKeys.PHRASE_SLOP);

    if (phraseSlop == null) {
      return 0;

    } else {
      return phraseSlop;
    }
  }

  /**
   * Set the minimum similarity for fuzzy queries. Default is defined on {@link
   * FuzzyQuery#defaultMaxEdits}.
   */
  @Override
  public void setFuzzyMinSim(float fuzzyMinSim) {
    QueryConfigHandler config = getQueryConfigHandler();
    FuzzyConfig fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG);

    if (fuzzyConfig == null) {
      fuzzyConfig = new FuzzyConfig();
      config.set(ConfigurationKeys.FUZZY_CONFIG, fuzzyConfig);
    }

    fuzzyConfig.setMinSimilarity(fuzzyMinSim);
  }

  /**
   * Sets the boost used for each field.
   *
   * @param boosts a collection that maps a field to its boost
   */
  public void setFieldsBoost(Map<String, Float> boosts) {
    getQueryConfigHandler().set(ConfigurationKeys.FIELD_BOOST_MAP, boosts);
  }

  /**
   * Returns the field to boost map used to set boost for each field.
   *
   * @return the field to boost map
   */
  public Map<String, Float> getFieldsBoost() {
    return getQueryConfigHandler().get(ConfigurationKeys.FIELD_BOOST_MAP);
  }

  /**
   * Sets the default {@link Resolution} used for certain field when no {@link Resolution} is
   * defined for this field.
   *
   * @param dateResolution the default {@link Resolution}
   */
  @Override
  public void setDateResolution(DateTools.Resolution dateResolution) {
    getQueryConfigHandler().set(ConfigurationKeys.DATE_RESOLUTION, dateResolution);
  }

  /**
   * Returns the default {@link Resolution} used for certain field when no {@link Resolution} is
   * defined for this field.
   *
   * @return the default {@link Resolution}
   */
  public DateTools.Resolution getDateResolution() {
    return getQueryConfigHandler().get(ConfigurationKeys.DATE_RESOLUTION);
  }

  /**
   * Returns the field to {@link Resolution} map used to normalize each date field.
   *
   * @return the field to {@link Resolution} map
   */
  public Map<CharSequence, DateTools.Resolution> getDateResolutionMap() {
    return getQueryConfigHandler().get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP);
  }

  /**
   * Sets the {@link Resolution} used for each field
   *
   * @param dateRes a collection that maps a field to its {@link Resolution}
   */
  public void setDateResolutionMap(Map<CharSequence, DateTools.Resolution> dateRes) {
    getQueryConfigHandler().set(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP, dateRes);
  }
}
