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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A TermFilteredPresearcher that indexes queries multiple times, with terms collected from
 * different routes through a querytree. Each route will produce a set of terms that are
 * *sufficient* to select the query, and are indexed into a separate, suffixed field.
 *
 * <p>Incoming documents are then converted to a set of Disjunction queries over each suffixed
 * field, and these queries are combined into a conjunction query, such that the document's set of
 * terms must match a term from each route.
 *
 * <p>This allows filtering out of documents that contain one half of a two-term phrase query, for
 * example. The query {@code "hello world"} will be indexed twice, once under 'hello' and once under
 * 'world'. A document containing the terms "hello there" would match the first field, but not the
 * second, and so would not be selected for matching.
 *
 * <p>The number of passes the presearcher makes is configurable. More passes will improve the
 * selected/matched ratio, but will take longer to index and will use more RAM.
 *
 * <p>A minimum weight can we set for terms to be chosen for the second and subsequent passes. This
 * allows users to avoid indexing stopwords, for example.
 */
public class MultipassTermFilteredPresearcher extends TermFilteredPresearcher {

  private final int passes;
  private final float minWeight;

  /**
   * Construct a new MultipassTermFilteredPresearcher
   *
   * @param passes the number of times a query should be indexed
   * @param minWeight the minimum weight a querytree should be advanced over
   * @param weightor the TreeWeightor to use
   * @param queryHandlers a list of custom query handlers
   * @param filterFields a set of fields to use as filters
   */
  public MultipassTermFilteredPresearcher(
      int passes,
      float minWeight,
      TermWeightor weightor,
      List<CustomQueryHandler> queryHandlers,
      Set<String> filterFields) {
    this(passes, minWeight, weightor, queryHandlers, filterFields, 0);
  }

  /**
   * Construct a new MultipassTermFilteredPresearcher that also filters on negated clauses
   *
   * @param passes the number of times a query should be indexed
   * @param minWeight the minimum weight a querytree should be advanced over. NotSlots usually weigh
   *     less than terms (see {@link TermWeightor#negate(double)}), so a minimum above 0 can keep
   *     later passes from using them
   * @param weightor the TreeWeightor to use
   * @param queryHandlers a list of custom query handlers
   * @param filterFields a set of fields to use as filters
   * @param maxNotSlots the maximum number of NotSlots a query may use per pass; 0 disables
   *     filtering on negated clauses. See {@link TermFilteredPresearcher}.
   */
  public MultipassTermFilteredPresearcher(
      int passes,
      float minWeight,
      TermWeightor weightor,
      List<CustomQueryHandler> queryHandlers,
      Set<String> filterFields,
      int maxNotSlots) {
    super(weightor, queryHandlers, filterFields, maxNotSlots);
    this.passes = passes;
    this.minWeight = minWeight;
  }

  /**
   * Construct a new MultipassTermFilteredPresearcher using {@link
   * TermFilteredPresearcher#DEFAULT_WEIGHTOR}
   *
   * <p>Note that this will be constructed with a minimum advance weight of zero
   *
   * @param passes the number of times a query should be indexed
   */
  public MultipassTermFilteredPresearcher(int passes) {
    this(passes, 0, DEFAULT_WEIGHTOR, Collections.emptyList(), Collections.emptySet());
  }

  @Override
  protected DocumentQueryBuilder getQueryBuilder() {
    return new MultipassDocumentQueryBuilder();
  }

  private static String field(String field, int pass) {
    return field + "_" + pass;
  }

  private class MultipassDocumentQueryBuilder implements DocumentQueryBuilder {

    Map<String, BytesRefHash> terms = new HashMap<>();
    Map<String, List<BytesRef>> negatableTerms = new HashMap<>();

    @Override
    public void addTerm(String field, BytesRef term) {
      BytesRefHash t = terms.computeIfAbsent(field, _ -> new BytesRefHash());
      t.add(term);
    }

    @Override
    public boolean supportsNotSlots() {
      return true;
    }

    @Override
    public void addNegatableTerm(String field, BytesRef term) {
      negatableTerms.computeIfAbsent(field, _ -> new ArrayList<>()).add(term);
    }

    @Override
    public Query build() {
      Map<String, List<BytesRef>> collectedTerms = new HashMap<>();
      for (Map.Entry<String, BytesRefHash> entry : terms.entrySet()) {
        collectedTerms.put(entry.getKey(), convertHash(entry.getValue()));
      }
      BooleanQuery.Builder parent = new BooleanQuery.Builder();
      for (int i = 0; i < passes; i++) {
        parent.add(toQuery(collectedTerms, negatableTerms, pass(i)), BooleanClause.Occur.MUST);
      }
      return parent.build();
    }
  }

  @Override
  public Document buildQueryDocument(QueryTree querytree) {
    Document doc = new Document();
    for (int i = 0; i < passes; i++) {
      for (IndexableField field : toDocument(querytree, pass(i))) {
        doc.add(field);
      }
      querytree.advancePhase(minWeight);
    }
    return doc;
  }

  /** The fields of pass {@code pass} */
  private static FieldMapper pass(int pass) {
    return field -> field(field, pass);
  }

  private static List<BytesRef> convertHash(BytesRefHash hash) {
    List<BytesRef> terms = new ArrayList<>(hash.size());
    for (int i = 0; i < hash.size(); i++) {
      terms.add(hash.get(i, new BytesRef()));
    }
    return terms;
  }
}
