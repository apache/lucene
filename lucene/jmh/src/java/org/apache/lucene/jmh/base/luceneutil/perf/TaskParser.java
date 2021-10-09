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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

/** The type Task parser. */
public class TaskParser {

  private final QueryParser queryParser;
  private final String fieldName;
  private final Sort titleDVSort;
  private final Sort titleBDVSort;
  private final Sort monthDVSort; // Month of the "last modified timestamp", SORTED doc values
  private final Sort
      dayOfYearDVSort; // Day of the year of the "last modified timestamp", NUMERIC doc values
  private final Sort lastModNDVSort;
  private final int topN;
  //  private final Random random;
  private final boolean doStoredLoads;
  private final IndexState state;
  private final VectorDictionary vectorDictionary;
  // private final String vectorField;

  /**
   * Instantiates a new Task parser.
   *
   * @param state the state
   * @param queryParser the query parser
   * @param fieldName the field name
   * @param topN the top n
   * @param random the random
   * @param vectorFile the vector file
   * @param doStoredLoads the do stored loads
   * @throws IOException the io exception
   */
  public TaskParser(
      IndexState state,
      QueryParser queryParser,
      String fieldName,
      int topN,
      Random random,
      String vectorFile,
      boolean doStoredLoads)
      throws IOException {
    this.queryParser = queryParser;
    this.fieldName = fieldName;
    this.topN = topN;
    //    this.random = random;
    this.doStoredLoads = doStoredLoads;
    this.state = state;
    if (vectorFile != null) {
      vectorDictionary = new VectorDictionary(vectorFile);
      // vectorField = "vector";
    } else {
      vectorDictionary = null;
      // vectorField = null;
    }
    titleDVSort = new Sort(new SortField("titleDV", SortField.Type.STRING));
    titleBDVSort = new Sort(new SortField("titleBDV", SortField.Type.STRING_VAL));
    monthDVSort = new Sort(new SortField("monthSortedDV", SortField.Type.STRING));
    dayOfYearDVSort = new Sort(new SortField("dayOfYearNumericDV", SortField.Type.INT));
    lastModNDVSort = new Sort(new SortField("lastModNDV", SortField.Type.LONG));
  }

  private static final Pattern filterPattern = Pattern.compile(" \\+filter=([0-9\\.]+)%");
  private static final Pattern minShouldMatchPattern =
      Pattern.compile(" \\+minShouldMatch=(\\d+)($| )");

  /**
   * Parse one task task.
   *
   * @param line the line
   * @return the task
   * @throws ParseException the parse exception
   */
  public Task parseOneTask(String line) throws ParseException {
    return new TaskBuilder(line).build();
  }

  /** The type Task builder. */
  class TaskBuilder {

    // --Commented out by Inspection START (10/7/21, 12:37 AM):
    //    /** The Line. */
    final String line;
    // --Commented out by Inspection STOP (10/7/21, 12:37 AM)
    /** The Category. */
    final String category;
    /** The Orig text. */
    final String origText;

    /** The Facets. */
    List<String> facets;
    /** The Text. */
    String text;
    /** The Do drill sideways. */
    boolean doDrillSideways,
        /** The Do hilite. */
        doHilite,
        /** The Do stored loads task. */
        doStoredLoadsTask;
    /** The Sort. */
    Sort sort;
    /** The Group. */
    String group;

    /**
     * Instantiates a new Task builder.
     *
     * @param line the line
     */
    TaskBuilder(String line) {
      this.line = line;

      final int spot = line.indexOf(':');
      if (spot == -1) {
        throw new RuntimeException("task line is malformed: " + line);
      }
      category = line.substring(0, spot);

      int spot2 = line.indexOf(" #");
      if (spot2 == -1) {
        spot2 = line.length();
      }

      origText = line.substring(spot + 1, spot2).trim();
    }

    /**
     * Build task.
     *
     * @return the task
     * @throws ParseException the parse exception
     */
    Task build() throws ParseException {
      if (category.equals("Respell")) {
        return new RespellTask(new Term(fieldName, origText));
      } else {
        if (origText.length() == 0) {
          throw new RuntimeException("null query line");
        }
        return buildQueryTask(origText);
      }
    }

    /**
     * Build query task task.
     *
     * @param input the input
     * @return the task
     * @throws ParseException the parse exception
     */
    Task buildQueryTask(String input) throws ParseException {
      text = input;
      facets = parseFacets();
      List<String> drillDowns = parseDrillDowns();
      doStoredLoadsTask = TaskParser.this.doStoredLoads;
      parseHilite();
      String[] taskAndType = parseTaskType(text);
      String taskType = taskAndType[0];
      text = taskAndType[1];
      int msm = parseMinShouldMatch();
      Query query = buildQuery(taskType, text, msm);
      Query query2 = applyDrillDowns(query, drillDowns);

      return new SearchTask(
          category,
          query2,
          sort,
          group,
          topN,
          doHilite,
          doStoredLoadsTask,
          facets,
          null,
          doDrillSideways);
    }

    /**
     * Parse task type string [ ].
     *
     * @param line the line
     * @return the string [ ]
     */
    String[] parseTaskType(String line) {
      int spot = line.indexOf("//");
      if (spot == -1) {
        return new String[] {"", line};
      } else {
        return new String[] {line.substring(0, spot), line.substring(spot + 2)};
      }
    }

    /**
     * Apply filter query.
     *
     * @param query the query
     * @param filter the filter
     * @return the query
     */
    Query applyFilter(Query query, Query filter) {
      if (filter == null) {
        return query;
      } else {
        return new Builder().add(query, Occur.MUST).add(filter, Occur.FILTER).build();
      }
    }

    /**
     * Apply drill downs query.
     *
     * @param query the query
     * @param drillDowns the drill downs
     * @return the query
     */
    Query applyDrillDowns(Query query, List<String> drillDowns) {
      if (drillDowns.isEmpty()) {
        return query;
      }
      DrillDownQuery q = new DrillDownQuery(state.facetsConfig, query);
      for (String s : drillDowns) {
        int i = s.indexOf('=');
        if (i == -1) {
          throw new IllegalArgumentException("drilldown is missing =");
        }
        String dim = s.substring(0, i);
        String values = s.substring(i + 1);

        while (true) {
          i = values.indexOf(',');
          if (i == -1) {
            q.add(dim, values);
            break;
          }
          q.add(dim, values.substring(0, i));
          values = values.substring(i + 1);
        }
      }
      return q;
    }

    /**
     * Parse filter query.
     *
     * @return the query
     */
    Query parseFilter() {
      // Check for filter (eg: " +filter=0.5%")
      final Matcher m = filterPattern.matcher(text);
      if (m.find()) {
        final double filterPct = Double.parseDouble(m.group(1));
        // Splice out the filter string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0))).trim();
        return new RandomQuery(filterPct);
      }
      return null;
    }

    /**
     * Parse min should match int.
     *
     * @return the int
     */
    int parseMinShouldMatch() {
      final Matcher m2 = minShouldMatchPattern.matcher(text);
      int minShouldMatch = 0;
      if (m2.find()) {
        minShouldMatch = Integer.parseInt(m2.group(1));
        // Splice out the minShouldMatch string:
        text = (text.substring(0, m2.start(0)) + text.substring(m2.end(0))).trim();
      }
      return minShouldMatch;
    }

    /**
     * Parse facets list.
     *
     * @return the list
     */
    List<String> parseFacets() {
      List<String> facets = new ArrayList<>();
      while (true) {
        int i = text.indexOf(" +facets:");
        if (i == -1) {
          break;
        }
        int j = text.indexOf(" ", i + 1);
        if (j == -1) {
          j = text.length();
        }
        String facetDim = text.substring(i + 9, j);
        int k = facetDim.indexOf(".");
        if (k == -1) {
          throw new IllegalArgumentException(
              "+facet:x should have format Dim.(taxonomy|sortedset); got: " + facetDim);
        }
        String s = facetDim.substring(0, k);
        if (state.facetFields.containsKey(s) == false) {
          throw new IllegalArgumentException("facetDim " + s + " was not indexed");
        }
        facets.add(facetDim);
        text = text.substring(0, i) + text.substring(j);
      }
      return facets;
    }

    /**
     * Parse drill downs list.
     *
     * @return the list
     */
    List<String> parseDrillDowns() {
      List<String> drillDowns = new ArrayList<>();
      // Eg: +drillDown:Date=2001,2004
      while (true) {
        int i = text.indexOf("+drillDown:");
        if (i == -1) {
          break;
        }
        int j = text.indexOf(" ", i);
        if (j == -1) {
          j = text.length();
        }

        String s = text.substring(i + 11, j);
        text = text.substring(0, i) + text.substring(j);

        drillDowns.add(s);
      }

      if (text.indexOf("+drillSideways") != -1) {
        text = text.replace("+drillSideways", "");
        doDrillSideways = true;
        if (drillDowns.size() == 0) {
          throw new RuntimeException(
              "cannot +drillSideways unless at least one +drillDown is defined");
        }
      } else {
        doDrillSideways = false;
      }
      return drillDowns;
    }

    /** Parse hilite. */
    void parseHilite() {
      if (text.startsWith("hilite//")) {
        doHilite = true;
        text = text.substring(8);

        // Highlighting does its own loading
        doStoredLoadsTask = false;
      } else {
        doHilite = false;
      }
    }

    /**
     * Build query query.
     *
     * @param type the type
     * @param text the text
     * @param minShouldMatch the min should match
     * @return the query
     * @throws ParseException the parse exception
     */
    Query buildQuery(String type, String text, int minShouldMatch) throws ParseException {
      Query query;
      switch (type) {
        case "ordered":
          return parseOrderedQuery();
        case "spanDis":
          return parseSpanDisjunctions();
        case "intervalDis":
          return parseIntervalDisjunctions(true);
        case "intervalDisMin":
          return parseIntervalDisjunctions(false);
        case "near":
          return parseNearQuery();
        case "multiPhrase":
          return parseMultiPhrase();
        case "disjunctionMax":
          return parseDisjunctionMax();
        case "nrq":
          return parseNRQ();
        case "datetimesort":
          throw new IllegalArgumentException("use lastmodndvsort instead");
        case "titlesort":
          throw new IllegalArgumentException("use titledvsort instead");
        case "vector":
          return parseVectorQuery();
        default:
          setSortAndGroup(type);
          query = queryParser.parse(text);
      }
      if (query.toString().equals("")) {
        throw new RuntimeException("query text \"" + text + "\" parsed to empty query");
      }
      if (minShouldMatch == 0) {
        return query;
      } else {
        if (!(query instanceof BooleanQuery)) {
          throw new RuntimeException(
              "minShouldMatch can only be used with BooleanQuery: query=" + origText);
        }
        Builder b = new Builder();
        b.setMinimumNumberShouldMatch(minShouldMatch);
        for (BooleanClause clause : ((BooleanQuery) query)) {
          b.add(clause);
        }
        return b.build();
      }
    }

    /**
     * Sets sort and group.
     *
     * @param taskType the task type
     */
    void setSortAndGroup(String taskType) {
      switch (taskType) {
        case "titledvsort":
          sort = titleDVSort;
          break;
        case "titlebdvsort":
          sort = titleBDVSort;
          break;
        case "monthdvsort":
          sort = monthDVSort;
          break;
        case "dayofyeardvsort":
          sort = dayOfYearDVSort;
          break;
        case "lastmodndvsort":
          sort = lastModNDVSort;
          break;
        case "group100":
          group = "group100";
          break;
        case "group10K":
          group = "group10K";
          break;
        case "group100K":
          group = "group100K";
          break;
        case "group1M":
          group = "group1M";
          break;
        case "groupblock1pass":
          group = "groupblock1pass";
          break;
        case "groupblock":
          group = "groupblock";
          break;
      }
    }

    /**
     * Parse nrq query.
     *
     * @return the query
     */
    Query parseNRQ() {
      // field start end
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      final int spot4 = text.indexOf(' ', spot3 + 1);
      if (spot4 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      final String nrqFieldName = text.substring(0, spot3);
      final int start = Integer.parseInt(text.substring(1 + spot3, spot4));
      final int end = Integer.parseInt(text.substring(1 + spot4));
      return IntPoint.newRangeQuery(nrqFieldName, start, end);
    }

    /**
     * Parse ordered query query.
     *
     * @return the query
     */
    Query parseOrderedQuery() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      return new IntervalQuery(
          fieldName,
          Intervals.maxwidth(
              10,
              Intervals.ordered(
                  Intervals.term(text.substring(0, spot3)),
                  Intervals.term(text.substring(spot3 + 1).trim()))));
    }

    /**
     * Parse near query query.
     *
     * @return the query
     */
    Query parseNearQuery() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      return new SpanNearQuery(
          new SpanQuery[] {
            new SpanTermQuery(new Term(fieldName, text.substring(0, spot3))),
            new SpanTermQuery(new Term(fieldName, text.substring(spot3 + 1).trim()))
          },
          10,
          true);
    }

    /**
     * Parse span disjunctions query.
     *
     * @return the query
     */
    Query parseSpanDisjunctions() {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[] {10}; // default to slop of 10
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      String field = fieldHolder[0];
      int slop = slopHolder[0];
      SpanQuery[] spanClauses =
          Arrays.stream(clauses)
              .map(
                  (component) -> {
                    SpanQuery[] disjunct =
                        Arrays.stream(component)
                            .map(
                                (words) -> {
                                  if (words.length == 1) {
                                    return new SpanTermQuery(new Term(field, words[0]));
                                  } else {
                                    return new SpanNearQuery(
                                        Arrays.stream(words)
                                            .map(
                                                (word) -> {
                                                  return new SpanTermQuery(new Term(field, word));
                                                })
                                            .toArray((size) -> new SpanQuery[size]),
                                        0,
                                        true);
                                  }
                                })
                            .toArray((size) -> new SpanQuery[size]);
                    return disjunct.length == 1 ? disjunct[0] : new SpanOrQuery(disjunct);
                  })
              .toArray((size) -> new SpanQuery[size]);
      // NOTE: in contrast to intervals (below), with spans there is no special
      // case for slop==0; we have only SpanNearQuery
      return spanClauses.length == 1 ? spanClauses[0] : new SpanNearQuery(spanClauses, slop, true);
    }

    /**
     * Parse interval disjunctions query.
     *
     * @param rewrite the rewrite
     * @return the query
     */
    Query parseIntervalDisjunctions(boolean rewrite) {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[] {10}; // default to slop of 10
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      String field = fieldHolder[0];
      int slop = slopHolder[0];
      IntervalsSource[] intervalClauses =
          Arrays.stream(clauses)
              .map(
                  (component) -> {
                    IntervalsSource[] disjunct =
                        Arrays.stream(component)
                            .map(
                                (words) -> {
                                  if (words.length == 1) {
                                    return Intervals.term(words[0]);
                                  } else {
                                    IntervalsSource[] intervalWords =
                                        Arrays.stream(words)
                                            .map(
                                                (word) -> {
                                                  return Intervals.term(word);
                                                })
                                            .toArray((size) -> new IntervalsSource[size]);
                                    return Intervals.phrase(intervalWords);
                                  }
                                })
                            .toArray((size) -> new IntervalsSource[size]);
                    return disjunct.length == 1 ? disjunct[0] : Intervals.or(rewrite, disjunct);
                  })
              .toArray((size) -> new IntervalsSource[size]);
      IntervalsSource positional;
      if (intervalClauses.length == 1) {
        // NOTE: apparently maxgaps/ordered/phrase do not rewrite for the single-clause
        // case? ... or in any event not in a way that's transparent immediately after
        // query construction. So we do it manually here, in order be sure that Intervals
        // can "put their best foot forward" on the plain-disjunction case.
        positional = intervalClauses[0];
      } else if (slop == 0) {
        // assumption: "phrase" is equivalent to "maxgaps(0, ordered)"?
        positional = Intervals.phrase(intervalClauses);
      } else {
        // the usual case
        positional = Intervals.maxgaps(slop, Intervals.ordered(intervalClauses));
      }
      return new IntervalQuery(field, positional);
    }

    /**
     * Parse multi phrase query.
     *
     * @return the query
     */
    Query parseMultiPhrase() {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[1]; // implicit default to slop=0
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      if (slopHolder[0] != 0) {
        throw new IllegalArgumentException(
            "multiPhrase only supports slop==0; found:" + slopHolder[0]);
      }
      String field = fieldHolder[0];
      MultiPhraseQuery.Builder b = new MultiPhraseQuery.Builder();
      for (int i = 0; i < clauses.length; i++) {
        String[][] words = clauses[i];
        Term[] terms = new Term[words.length];
        for (int j = 0; j < words.length; j++) {
          terms[j] = new Term(field, words[j][0]);
        }
        b.add(terms);
      }
      return b.build();
    }

    private String[][][] parseDisjunctionSpec(String[] fieldHolder, int[] slopHolder) {
      int colon = text.indexOf(':');
      if (colon == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      fieldHolder[0] = text.substring("(".length(), colon);
      //      MultiPhraseQuery.Builder b = new MultiPhraseQuery.Builder();
      int endParen = text.indexOf(')');
      if (endParen == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      int checkExplicitSlop = endParen + 1;
      if (text.length() > checkExplicitSlop && text.charAt(checkExplicitSlop) == '~') {
        slopHolder[0] =
            Integer.parseInt(text.substring(checkExplicitSlop + 1).split("[^0-9]", 2)[0]);
      }
      String queryText = text.substring(colon + 1, endParen);
      return Arrays.stream(queryText.split("\\s+"))
          .map(
              (clause) -> {
                return Arrays.stream(clause.split("\\|"))
                    .map(
                        (component) -> {
                          return component.split("-");
                        })
                    .toArray((size) -> new String[size][]);
              })
          .toArray((size) -> new String[size][][]);
    }

    /**
     * Parse disjunction max query.
     *
     * @return the query
     */
    Query parseDisjunctionMax() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      List<Query> clauses = new ArrayList<Query>();
      clauses.add(new TermQuery(new Term(fieldName, text.substring(0, spot3))));
      clauses.add(new TermQuery(new Term(fieldName, text.substring(spot3 + 1).trim())));
      return new DisjunctionMaxQuery(clauses, 0.1f);
    }

    /**
     * Parse vector query query.
     *
     * @return the query
     */
    Query parseVectorQuery() {
      float[] queryVector = vectorDictionary.computeTextVector(text);
      return new KnnQuery("vector", text, queryVector, topN);
    }
  }
}
