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
import java.util.TimeZone;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;

/** Configuration options common across queryparser implementations. */
public interface CommonQueryParserConfiguration {

  /**
   * Set to <code>true</code> to allow leading wildcard characters.
   *
   * <p>When set, <code>*</code> or <code>?</code> are allowed as the first character of a
   * PrefixQuery and WildcardQuery. Note that this can produce very slow queries on big indexes.
   *
   * <p>Default: false.
   */
  public void setAllowLeadingWildcard(boolean allowLeadingWildcard);

  /**
   * Set to <code>true</code> to enable position increments in result query.
   *
   * <p>When set, result phrase and multi-phrase queries will be aware of position increments.
   * Useful when e.g. a StopFilter increases the position increment of the token that follows an
   * omitted token.
   *
   * <p>Default: false.
   */
  public void setEnablePositionIncrements(boolean enabled);

  /**
   * @see #setEnablePositionIncrements(boolean)
   */
  public boolean getEnablePositionIncrements();

  /**
   * By default QueryParser uses {@link
   * org.apache.lucene.search.MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE} when creating a {@link
   * PrefixQuery}, {@link WildcardQuery} or {@link TermRangeQuery}. This implementation is generally
   * preferable because it a) Runs faster b) Does not have the scarcity of terms unduly influence
   * score c) avoids any {@link org.apache.lucene.search.IndexSearcher.TooManyClauses} exception.
   * However, if your application really needs to use the old-fashioned {@link BooleanQuery}
   * expansion rewriting and the above points are not relevant then use this to change the rewrite
   * method. As another alternative, if you prefer all terms to be rewritten as a filter up-front,
   * you can use {@link org.apache.lucene.search.MultiTermQuery#CONSTANT_SCORE_REWRITE}. For more
   * information on the different rewrite methods available, see {@link
   * org.apache.lucene.search.MultiTermQuery} documentation.
   */
  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method);

  /**
   * @see #setMultiTermRewriteMethod(org.apache.lucene.search.MultiTermQuery.RewriteMethod)
   */
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod();

  /**
   * Set the prefix length for fuzzy queries. Default is 0.
   *
   * @param fuzzyPrefixLength The fuzzyPrefixLength to set.
   */
  public void setFuzzyPrefixLength(int fuzzyPrefixLength);

  /** Set locale used by date range parsing. */
  public void setLocale(Locale locale);

  /** Returns current locale, allowing access by subclasses. */
  public Locale getLocale();

  public void setTimeZone(TimeZone timeZone);

  public TimeZone getTimeZone();

  /**
   * Sets the default slop for phrases. If zero, then exact phrase matches are required. Default
   * value is zero.
   */
  public void setPhraseSlop(int defaultPhraseSlop);

  public Analyzer getAnalyzer();

  /**
   * @see #setAllowLeadingWildcard(boolean)
   */
  public boolean getAllowLeadingWildcard();

  /** Get the minimal similarity for fuzzy queries. */
  public float getFuzzyMinSim();

  /**
   * Get the prefix length for fuzzy queries.
   *
   * @return Returns the fuzzyPrefixLength.
   */
  public int getFuzzyPrefixLength();

  /** Gets the default slop for phrases. */
  public int getPhraseSlop();

  /**
   * Set the minimum similarity for fuzzy queries. Default is defined on {@link
   * FuzzyQuery#defaultMaxEdits}.
   */
  public void setFuzzyMinSim(float fuzzyMinSim);

  /**
   * Sets the default {@link Resolution} used for certain field when no {@link Resolution} is
   * defined for this field.
   *
   * @param dateResolution the default {@link Resolution}
   */
  public void setDateResolution(DateTools.Resolution dateResolution);
}
