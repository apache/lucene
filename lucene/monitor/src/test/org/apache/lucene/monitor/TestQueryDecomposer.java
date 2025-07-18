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

package org.apache.lucene.monitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TestQueryDecomposer extends MonitorTestBase {

  private static final QueryDecomposer decomposer = new QueryDecomposer();

  public void testConjunctionsAreNotDecomposed() {
    Query q = parse("+hello world");
    Set<Query> expected = Collections.singleton(parse("+hello world"));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testSimpleDisjunctions() {
    Query q = parse("hello world");
    List<Query> expected = Arrays.asList(parse("world"), parse("hello"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testNestedDisjunctions() {
    Query q = parse("(hello goodbye) world");
    List<Query> expected = Arrays.asList(parse("world"), parse("hello"), parse("goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testExclusions() {
    List<Query> expected = Arrays.asList(parse("+world -goodbye"), parse("+hello -goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(parse("hello world -goodbye"))));
  }

  public void testNestedExclusions() {
    List<Query> expected =
        Arrays.asList(parse("+(+world -goodbye) -greeting"), parse("+(+hello -goodbye) -greeting"));
    assertEquals(
        expected,
        new ArrayList<>(decomposer.decompose(parse("((hello world) -goodbye) -greeting"))));
  }

  public void testSingleValuedConjunctions() {
    List<Query> expected = Arrays.asList(parse("world"), parse("hello"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(parse("+(hello world)"))));
  }

  public void testSingleValuedConjunctWithExclusions() {
    List<Query> expected = Arrays.asList(parse("+world -goodbye"), parse("+hello -goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(parse("+(hello world) -goodbye"))));
  }

  public void testBoostsArePreserved() {
    List<Query> expected = Arrays.asList(parse("world^0.7"), parse("hello^0.7"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(parse("+(hello world)^0.7"))));
    expected = Arrays.asList(parse("+world^0.7 -goodbye"), parse("+hello^0.7 -goodbye"));
    assertEquals(
        expected, new ArrayList<>(decomposer.decompose(parse("+(hello world)^0.7 -goodbye"))));
    expected = Arrays.asList(parse("world^0.8"), parse("(hello^0.5)^0.8"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(parse("+(hello^0.5 world)^0.8"))));
  }

  public void testDisjunctionMaxDecomposition() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))),
            0.1f);
    List<Query> expected = Arrays.asList(parse("f:t1"), parse("f:t2"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testNestedDisjunctionMaxDecomposition() {
    Query q = new DisjunctionMaxQuery(Arrays.asList(parse("hello goodbye"), parse("world")), 0.1f);
    List<Query> expected = Arrays.asList(parse("world"), parse("hello"), parse("goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testFilterAndShouldClause() {
    final Query shouldTermQuery = new TermQuery(new Term("f", "should"));
    final Query filterTermQuery = new TermQuery(new Term("f", "filter"));
    Query q =
        new BooleanQuery.Builder()
            .add(shouldTermQuery, BooleanClause.Occur.SHOULD)
            .add(filterTermQuery, BooleanClause.Occur.FILTER)
            .build();

    assertEquals(Collections.singleton(q), decomposer.decompose(q));
  }

  public void testDisjunctionCollapsesDistinct() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(parse("hello goodbye"), parse("goodbye hello")), 0.1f);
    List<Query> expected = Arrays.asList(parse("field:hello"), parse("field:goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testDisjunctionWithBoostCollapsesDistinct() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(parse("hello^0.7 goodbye^0.6"), parse("goodbye^0.6 hello^0.7")), 0.1f);
    List<Query> expected = Arrays.asList(parse("(field:hello)^0.7"), parse("(field:goodbye)^0.6"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testDisjunctionWithNoOpBoostCollapsesDistinct() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(parse("hello^1.0 goodbye^1.0"), parse("goodbye^1.0 hello^1.0")), 0.1f);
    List<Query> expected = Arrays.asList(parse("field:hello"), parse("field:goodbye"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testDisjunctionTermEquivalentGroupingOfDistinctQueries() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(parse("hello^0.7 goodbye^0.6"), parse("goodbye^0.7 hello^0.6")), 0.1f);
    List<Query> expected =
        Arrays.asList(
            parse("(field:hello)^0.7 (field:hello)^0.6"),
            parse("(field:goodbye)^0.6 (field:goodbye)^0.7"));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testBooleanTermEquivalentGroupingOfDistinctQueries() {
    Query q = parse("(hello OR (goodbye AND test) OR (world OR (\"test goodbye\")))");
    List<Query> expected =
        Arrays.asList(
            parse("field:world"),
            parse("field:hello"),
            parse("(+field:goodbye +field:test) field:\"test goodbye\""));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }

  public void testBooleanTermEquivalentGroupingRespectsFields() {
    Query q = parse("(hello OR (goodbye AND test) OR (world OR (f:\"test goodbye\")))");
    List<Query> expected =
        Arrays.asList(
            parse("field:world"),
            parse("field:hello"),
            parse("(+field:goodbye +field:test)"),
            parse("f:\"test goodbye\""));
    assertEquals(expected, new ArrayList<>(decomposer.decompose(q)));
  }
}
