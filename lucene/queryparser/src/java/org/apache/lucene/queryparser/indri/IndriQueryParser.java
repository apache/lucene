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
package org.apache.lucene.queryparser.indri;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndriAndQuery;
import org.apache.lucene.search.IndriOrQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * IndriQueryParser is used to parse human readable query syntax and create Indri queries.
 *
 * <p><b>Query Operators</b>
 *
 * <ul>
 *   <li>'{@code #and(t1 t2)}' specifies {@code IndriAnd} operation: <code>#and(term1 term2)</code>
 *   <li>'{@code #or(t1 t2)}' specifies {@code IndriOr} operation: <code>#or(term1 term2)</code>
 *   <li>'{@code #wsum(boost1 token1 boost2 token2)}' specifies {@code IndriOr} operation: <code>
 *       #wsum(1.0 term1 2.0 term2)</code>
 * </ul>
 *
 * <p>The default operator is {@code IndriAnd} if no other operator is specified. For example, the
 * following will {@code IndriAnd} {@code token1} and {@code token2} together: <code>token1 token2
 * </code>
 */
public class IndriQueryParser {

  private static final String AND = "and";
  private static final String OR = "or";
  private static final String WAND = "wand";
  private static final String WEIGHT = "weight";

  private final Analyzer analyzer;
  private String field;

  public IndriQueryParser(Analyzer analyzer, String field) throws IOException {
    this.analyzer = analyzer;
    this.field = field;
  }

  /**
   * Count the number of occurrences of character c in string s.
   *
   * @param c A character.
   * @param s A string.
   */
  private static int countChars(String s, char c) {
    int count = 0;

    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == c) {
        count++;
      }
    }
    return count;
  }

  /**
   * Get the index of the right parenenthesis that balances the left-most parenthesis. Return -1 if
   * it doesn't exist.
   *
   * @param s A string containing a query.
   */
  private static int indexOfBalencingParen(String s) {
    int depth = 0;

    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == '(') {
        depth++;
      } else if (s.charAt(i) == ')') {
        depth--;

        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
  }

  private QueryParserOperatorQuery createOperator(String operatorName, Occur occur) {
    QueryParserOperatorQuery operatorQuery = new QueryParserOperatorQuery();

    int operatorDistance = 0;
    String operatorNameLowerCase = new String(operatorName).toLowerCase(Locale.ROOT);
    operatorNameLowerCase = operatorNameLowerCase.replace("#", "");
    operatorNameLowerCase = operatorNameLowerCase.replace("~", "");

    operatorQuery.setOperator(operatorNameLowerCase);
    operatorQuery.setField(field);
    operatorQuery.setDistance(operatorDistance);
    operatorQuery.setOccur(occur);

    return operatorQuery;
  }

  private static class PopWeight {
    private Float weight;
    private String queryString;

    public Float getWeight() {
      return weight;
    }

    public void setWeight(Float weight) {
      this.weight = weight;
    }

    public String getQueryString() {
      return queryString;
    }

    public void setQueryString(String queryString) {
      this.queryString = queryString;
    }
  }

  /**
   * Remove a weight from an argument string. Return the weight and the modified argument string.
   */
  private PopWeight popWeight(String argString) {

    String[] substrings = argString.split("[ \t]+", 2);

    if (substrings.length < 2) {
      syntaxError("Missing weight or query argument");
    }

    PopWeight popWeight = new PopWeight();
    popWeight.setWeight(Float.valueOf(substrings[0]));
    popWeight.setQueryString(substrings[1]);

    return popWeight;
  }

  /**
   * Remove a subQuery from an argument string. Return the subquery and the modified argument
   * string.
   */
  private String popSubquery(
      String argString, QueryParserOperatorQuery queryTree, Float weight, Occur occur) {

    int i = indexOfBalencingParen(argString);

    if (i < 0) { // Query syntax error. The parser
      i = argString.length(); // handles it. Here, just don't fail.
    }

    String subquery = argString.substring(0, i + 1);
    queryTree.addSubquery(parseQueryString(subquery, occur), weight);

    argString = argString.substring(i + 1);

    return argString;
  }

  /** Remove a term from an argument string. Return the term and the modified argument string. */
  private String popTerm(
      String argString, QueryParserOperatorQuery queryTree, Float weight, Occur occur) {
    String[] substrings = argString.split("[ \t\n\r]+", 2);
    String token = substrings[0];

    // Split the token into a term and a field.
    int delimiter = token.indexOf('.');
    String term = null;

    if (delimiter < 0) {
      term = token;
    } else { // Remove the field from the token
      field = token.substring(delimiter + 1).toLowerCase(Locale.ROOT);
      term = token.substring(0, delimiter);
    }

    final BytesRef normalizedTerm = analyzer.normalize(field, term);
    QueryParserTermQuery termQuery = new QueryParserTermQuery();
    termQuery.setTerm(normalizedTerm.utf8ToString());
    termQuery.setField(field);
    termQuery.setOccur(occur);
    queryTree.addSubquery(termQuery, weight);

    if (substrings.length < 2) { // Is this the last argument?
      argString = "";
    } else {
      argString = substrings[1];
    }

    return argString;
  }

  private QueryParserQuery parseQueryString(String queryString, Occur occur) {
    // Create the query tree
    // This simple parser is sensitive to parenthensis placement, so
    // check for basic errors first.
    queryString = queryString.trim(); // The last character should be ')'

    if ((countChars(queryString, '(') == 0)
        || (countChars(queryString, '(') != countChars(queryString, ')'))
        || (indexOfBalencingParen(queryString) != (queryString.length() - 1))) {
      // throw IllegalArgumentException("Missing, unbalanced, or misplaced
      // parentheses");
    }

    // The query language is prefix-oriented, so the query string can
    // be processed left to right. At each step, a substring is
    // popped from the head (left) of the string, and is converted to
    // a Qry object that is added to the query tree. Subqueries are
    // handled via recursion.

    // Find the left-most query operator and start the query tree.
    String[] substrings = queryString.split("[(]", 2);
    String queryOperator = AND;
    if (substrings.length > 1) {
      queryOperator = substrings[0].trim();
    }
    QueryParserOperatorQuery queryTree = createOperator(queryOperator, occur);

    // Start consuming queryString by removing the query operator and
    // its terminating ')'. queryString is always the part of the
    // query that hasn't been processed yet.

    if (substrings.length > 1) {
      queryString = substrings[1];
      queryString = queryString.substring(0, queryString.lastIndexOf(")")).trim();
    }

    // Each pass below handles one argument to the query operator.
    // Note: An argument can be a token that produces multiple terms
    // (e.g., "near-death") or a subquery (e.g., "#and (a b c)").
    // Recurse on subqueries.

    while (queryString.length() > 0) {

      // If the operator uses weighted query arguments, each pass of
      // this loop must handle "weight arg". Handle the weight first.

      Float weight = null;
      if (queryTree.getOperator().equals(WEIGHT) || queryTree.getOperator().equals(WAND)) {
        PopWeight popWeight = popWeight(queryString);
        weight = popWeight.getWeight();
        queryString = popWeight.getQueryString();
      }

      // Now handle the argument (which could be a subquery).
      if (queryString.charAt(0) == '#' || queryString.charAt(0) == '~') { // Subquery
        queryString = popSubquery(queryString, queryTree, weight, occur).trim();
        occur = Occur.SHOULD;
      } else { // Term
        queryString = popTerm(queryString, queryTree, weight, occur);
        occur = Occur.SHOULD;
      }
    }

    return queryTree;
  }

  public Query parseQuery(String queryString) {
    // TODO: json or indri query
    queryString = queryString.replace("'", "");
    queryString = queryString.replace("\"", "");
    queryString = queryString.replace("+", " ");
    queryString = queryString.replace(":", ".");
    QueryParserQuery qry = parseQueryString(queryString, Occur.SHOULD);
    return getLuceneQuery(qry);
  }

  private Query getLuceneQuery(QueryParserQuery queryTree) {
    BooleanClause clause = createBooleanClause(queryTree);
    Query query = null;
    if (clause != null) {
      query = clause.getQuery();
    }
    return query;
  }

  public BooleanClause createBooleanClause(QueryParserQuery queryTree) {
    Query query = null;
    if (queryTree instanceof QueryParserOperatorQuery) {
      QueryParserOperatorQuery operatorQuery = (QueryParserOperatorQuery) queryTree;

      // Create clauses for subqueries
      List<BooleanClause> clauses = new ArrayList<>();
      if (operatorQuery.getSubqueries() != null) {
        for (QueryParserQuery subquery : operatorQuery.getSubqueries()) {
          BooleanClause clause = createBooleanClause(subquery);
          if (clause != null) {
            clauses.add(clause);
          }
        }

        // Create Operator
        if (operatorQuery.getOperator().equalsIgnoreCase(OR)) {
          query = new IndriOrQuery(clauses);
        } else if (operatorQuery.getOperator().equalsIgnoreCase(WAND)) {
          query = new IndriAndQuery(clauses);
        } else {
          query = new IndriAndQuery(clauses);
        }
      }
    } else if (queryTree instanceof QueryParserTermQuery) {
      // Create term query
      QueryParserTermQuery termQuery = (QueryParserTermQuery) queryTree;
      String field = "all";
      if (termQuery.getField() != null) {
        field = termQuery.getField();
      }
      query = new TermQuery(new Term(field, termQuery.getTerm()));
    }
    if (queryTree.getBoost() != null && query != null) {
      query = new BoostQuery(query, queryTree.getBoost().floatValue());
    }
    BooleanClause clause = null;
    if (query != null) {
      clause = new BooleanClause(query, queryTree.getOccur());
    }
    return clause;
  }

  /**
   * Throw an error specialized for query parsing syntax errors.
   *
   * @param errorString The string "Syntax
   * @throws IllegalArgumentException The query contained a syntax error
   */
  private static void syntaxError(String errorString) throws IllegalArgumentException {
    throw new IllegalArgumentException("Syntax Error: " + errorString);
  }
}
