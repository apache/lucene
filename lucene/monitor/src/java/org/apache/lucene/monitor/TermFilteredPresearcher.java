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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Presearcher implementation that uses terms extracted from queries to index them in the Monitor,
 * and builds a disjunction from terms in a document to match them.
 *
 * <p>Handling of queries that do not support term extraction through the {@link
 * org.apache.lucene.search.QueryVisitor} API can be configured by passing a list of {@link
 * CustomQueryHandler} implementations.
 *
 * <p>Filtering by additional fields can be configured by passing a set of field names. Documents
 * that contain values in those fields will only be checked against {@link MonitorQuery} instances
 * that have the same fieldname-value mapping in their metadata.
 *
 * <p>With a maximum number of negative disjuncts greater than 0, negated terms filter too: {@code
 * *:* -flag:true} is then only selected for documents without {@code flag:true}. Negations that
 * cannot be expressed exactly, such as phrases or wildcards, still select the query for every
 * document, and in a batch a term only excludes queries if every document contains it. Field names
 * containing {@code __not} are reserved. Changing the setting requires re-indexing the queries.
 */
public class TermFilteredPresearcher extends Presearcher {

  /** The default TermWeightor, weighting by token length */
  public static final TermWeightor DEFAULT_WEIGHTOR = TermWeightor.DEFAULT;

  private final QueryAnalyzer extractor;
  private final TermWeightor weightor;

  private final Set<String> filterFields;
  private final List<CustomQueryHandler> queryHandlers = new ArrayList<>();
  private final int maxNegativeDisjuncts;

  static final String NOT_SUFFIX = "__not";
  static final String NEGATIVE_DISJUNCT_MARKER_FIELD = "__negdisjunct";
  static final BytesRef NEGATIVE_DISJUNCT_MARKER = new BytesRef("__T__");

  public static final String ANYTOKEN_FIELD = "__anytokenfield";
  public static final String ANYTOKEN = "__ANYTOKEN__";

  /** Creates a new TermFilteredPresearcher using the default term weighting */
  public TermFilteredPresearcher() {
    this(DEFAULT_WEIGHTOR, Collections.emptyList(), Collections.emptySet());
  }

  /**
   * Creates a new TermFilteredPresearcher
   *
   * @param weightor the TermWeightor
   * @param customQueryHandlers A list of custom query handlers to extract terms from non-core
   *     queries
   * @param filterFields A set of fields to filter on
   */
  public TermFilteredPresearcher(
      TermWeightor weightor,
      List<CustomQueryHandler> customQueryHandlers,
      Set<String> filterFields) {
    this(weightor, customQueryHandlers, filterFields, 0);
  }

  /**
   * Creates a new TermFilteredPresearcher
   *
   * @param weightor the TermWeightor
   * @param customQueryHandlers A list of custom query handlers to extract terms from non-core
   *     queries
   * @param filterFields A set of fields to filter on
   * @param maxNegativeDisjuncts the maximum number of negative disjuncts a query may use per pass;
   *     0 disables filtering on negated clauses
   */
  public TermFilteredPresearcher(
      TermWeightor weightor,
      List<CustomQueryHandler> customQueryHandlers,
      Set<String> filterFields,
      int maxNegativeDisjuncts) {
    if (maxNegativeDisjuncts < 0) {
      throw new IllegalArgumentException(
          "maxNegativeDisjuncts must be >= 0, got " + maxNegativeDisjuncts);
    }
    this.extractor = new QueryAnalyzer(customQueryHandlers, maxNegativeDisjuncts > 0);
    this.filterFields = filterFields;
    this.queryHandlers.addAll(customQueryHandlers);
    this.weightor = weightor;
    this.maxNegativeDisjuncts = maxNegativeDisjuncts;
  }

  @Override
  public final Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor) {
    try {
      DocumentQueryBuilder queryBuilder = getQueryBuilder();
      if (maxNegativeDisjuncts > 0 && !queryBuilder.supportsNegativeDisjuncts()) {
        throw new IllegalStateException(
            "Negative disjuncts are enabled, but the DocumentQueryBuilder does not support them");
      }
      for (FieldInfo field : reader.getFieldInfos()) {
        if (filterFields.contains(field.name)) {
          continue;
        }
        Terms terms = reader.terms(field.name);
        if (terms == null) {
          continue;
        }

        TokenStream ts = new TermsEnumTokenStream(terms.iterator());
        for (CustomQueryHandler handler : queryHandlers) {
          ts = handler.wrapTermStream(field.name, ts);
        }
        addTerms(queryBuilder, field.name, ts, termAcceptor);
        if (maxNegativeDisjuncts > 0) {
          addNegatableTerms(
              queryBuilder, field.name, terms.iterator(), reader.maxDoc(), termAcceptor);
        }
      }
      Query presearcherQuery = queryBuilder.build();

      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(presearcherQuery, BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term(ANYTOKEN_FIELD, ANYTOKEN)), BooleanClause.Occur.SHOULD);
      presearcherQuery = bq.build();
      if (filterFields.isEmpty() == false) {
        bq = new BooleanQuery.Builder();
        bq.add(presearcherQuery, BooleanClause.Occur.MUST);
        Query filterQuery = buildFilterFields(reader);
        if (filterQuery != null) {
          bq.add(filterQuery, BooleanClause.Occur.FILTER);
          presearcherQuery = bq.build();
        }
      }
      return presearcherQuery;
    } catch (IOException e) {
      // We're a MemoryIndex, so this shouldn't happen...
      throw new RuntimeException(e);
    }
  }

  /** Adds the terms of a field that the term filter accepts, to select queries */
  private static void addTerms(
      DocumentQueryBuilder queryBuilder,
      String field,
      TokenStream ts,
      BiPredicate<String, BytesRef> termAcceptor)
      throws IOException {
    TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
    while (ts.incrementToken()) {
      BytesRef term = termAtt.getBytesRef();
      if (termAcceptor.test(field, term)) {
        queryBuilder.addTerm(field, BytesRef.deepCopyOf(term));
      }
    }
    ts.close();
  }

  /**
   * Adds the terms every document contains, which can exclude queries through a negative disjunct
   */
  private static void addNegatableTerms(
      DocumentQueryBuilder queryBuilder,
      String field,
      TermsEnum terms,
      int maxDoc,
      BiPredicate<String, BytesRef> termAcceptor)
      throws IOException {
    // read directly: TermsEnumTokenStream can merge invalid UTF-8 terms, which must not exclude
    String notField = field + NOT_SUFFIX;
    for (BytesRef term = terms.next(); term != null; term = terms.next()) {
      if (terms.docFreq() == maxDoc && termAcceptor.test(notField, term)) {
        queryBuilder.addNegatableTerm(field, BytesRef.deepCopyOf(term));
      }
    }
  }

  private Query buildFilterFields(LeafReader reader) throws IOException {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String field : filterFields) {
      Query q = buildFilterClause(reader, field);
      if (q != null) {
        builder.add(q, BooleanClause.Occur.MUST);
      }
    }
    BooleanQuery bq = builder.build();
    if (bq.clauses().size() == 0) {
      return null;
    }
    return bq;
  }

  private Query buildFilterClause(LeafReader reader, String field) throws IOException {

    Terms terms = reader.terms(field);
    if (terms == null) return null;

    BooleanQuery.Builder bq = new BooleanQuery.Builder();

    int docsInBatch = reader.maxDoc();

    BytesRef term;
    TermsEnum te = terms.iterator();
    while ((term = te.next()) != null) {
      // we need to check that every document in the batch has the same field values, otherwise
      // this filtering will not work
      if (te.docFreq() != docsInBatch)
        throw new IllegalArgumentException(
            "Some documents in this batch do not have a term value of "
                + field
                + ":"
                + Term.toString(term));
      bq.add(new TermQuery(new Term(field, term)), BooleanClause.Occur.SHOULD);
    }

    BooleanQuery built = bq.build();

    if (built.clauses().size() == 0) return null;

    return built;
  }

  /** Constructs a document disjunction from a set of terms */
  protected interface DocumentQueryBuilder {

    /** Add a term from this document */
    void addTerm(String field, BytesRef term) throws IOException;

    /**
     * Whether this builder adds negative disjunct clauses; required when negative disjuncts are
     * enabled
     */
    default boolean supportsNegativeDisjuncts() {
      return false;
    }

    /** Add a term that every document in the batch contains, to exclude queries by */
    default void addNegatableTerm(String field, BytesRef term) throws IOException {
      throw new UnsupportedOperationException(
          "supportsNegativeDisjuncts() returns true, but addNegatableTerm is not implemented");
    }

    /**
     * @return the final Query
     */
    Query build();
  }

  /** Returns a {@link DocumentQueryBuilder} for this presearcher */
  protected DocumentQueryBuilder getQueryBuilder() {
    return new DocumentQueryBuilder() {

      Map<String, List<BytesRef>> terms = new HashMap<>();
      Map<String, List<BytesRef>> negatableTerms = new HashMap<>();

      @Override
      public void addTerm(String field, BytesRef term) {
        List<BytesRef> t = terms.computeIfAbsent(field, _ -> new ArrayList<>());
        t.add(term);
      }

      @Override
      public boolean supportsNegativeDisjuncts() {
        return true;
      }

      @Override
      public void addNegatableTerm(String field, BytesRef term) {
        negatableTerms.computeIfAbsent(field, _ -> new ArrayList<>()).add(term);
      }

      @Override
      public Query build() {
        return toQuery(terms, negatableTerms, field -> field);
      }
    };
  }

  /** Renames the fields a query is indexed under, so that a subclass can index it more than once */
  @FunctionalInterface
  interface FieldMapper {
    String map(String field);
  }

  /**
   * The field that holds the terms of negative disjunct {@code i}, for a field named {@code field}
   */
  static String negativeDisjunctField(String field, int i) {
    return field + NOT_SUFFIX + "_" + i;
  }

  private static String negativeDisjunctMarkerField(FieldMapper fields, int i) {
    return fields.map(NEGATIVE_DISJUNCT_MARKER_FIELD) + "_" + i;
  }

  /** The document query matching queries indexed by {@link #toDocument} under {@code fields} */
  Query toQuery(
      Map<String, ? extends Collection<BytesRef>> terms,
      Map<String, ? extends Collection<BytesRef>> negatableTerms,
      FieldMapper fields) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (Map.Entry<String, ? extends Collection<BytesRef>> entry : terms.entrySet()) {
      builder.add(
          new TermInSetQuery(fields.map(entry.getKey()), entry.getValue()),
          BooleanClause.Occur.SHOULD);
    }
    for (int i = 0; i < maxNegativeDisjuncts; i++) {
      BooleanQuery.Builder negativeDisjunct = new BooleanQuery.Builder();
      negativeDisjunct.add(
          new TermQuery(new Term(negativeDisjunctMarkerField(fields, i), NEGATIVE_DISJUNCT_MARKER)),
          BooleanClause.Occur.FILTER);
      for (Map.Entry<String, ? extends Collection<BytesRef>> entry : negatableTerms.entrySet()) {
        negativeDisjunct.add(
            new TermInSetQuery(
                negativeDisjunctField(fields.map(entry.getKey()), i), entry.getValue()),
            BooleanClause.Occur.MUST_NOT);
      }
      builder.add(negativeDisjunct.build(), BooleanClause.Occur.SHOULD);
    }
    return builder.build();
  }

  static final FieldType QUERYFIELDTYPE;

  static {
    QUERYFIELDTYPE = new FieldType(TextField.TYPE_NOT_STORED);
    QUERYFIELDTYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    QUERYFIELDTYPE.freeze();
  }

  @Override
  public final Document indexQuery(Query query, Map<String, String> metadata) {
    QueryTree querytree = extractor.buildTree(query, weightor);
    Document doc = buildQueryDocument(querytree);
    for (String field : filterFields) {
      if (metadata != null && metadata.containsKey(field)) {
        doc.add(new TextField(field, metadata.get(field), Field.Store.YES));
      }
    }
    return doc;
  }

  /** Builds a {@link Document} from the terms extracted from a query */
  protected Document buildQueryDocument(QueryTree querytree) {
    return toDocument(querytree, field -> field);
  }

  /** Indexes the terms and negative disjuncts of the current path through a {@link QueryTree} */
  Document toDocument(QueryTree querytree, FieldMapper fields) {
    Document doc = new Document();
    CollectedTerms collected = collectTermsAndNegativeDisjuncts(querytree);
    List<Set<Term>> negativeDisjuncts = collected.negativeDisjuncts();
    if (negativeDisjuncts.size() > maxNegativeDisjuncts) {
      // More negative disjuncts than the document side checks: the marker alone satisfies the path
      // for
      // every document, so its terms are not needed either
      addField(doc, negativeDisjunctMarkerField(fields, 0), NEGATIVE_DISJUNCT_MARKER);
      return doc;
    }
    for (Map.Entry<String, BytesRefHash> entry : collected.terms().entrySet()) {
      if (maxNegativeDisjuncts > 0 && entry.getKey().contains(NOT_SUFFIX)) {
        // The field could be indexed under the name of a negative disjunct field, whose clauses
        // exclude
        // queries, so select the query for every document instead
        addField(doc, ANYTOKEN_FIELD, new BytesRef(ANYTOKEN));
        continue;
      }
      String field = fields.map(entry.getKey());
      addField(doc, field, entry.getValue());
      if (!field.equals(entry.getKey())) {
        // the term filter checks document terms under their own field names
        addField(doc, entry.getKey(), entry.getValue());
      }
    }
    for (int i = 0; i < negativeDisjuncts.size(); i++) {
      Map<String, BytesRefHash> byField = new HashMap<>();
      for (Term term : negativeDisjuncts.get(i)) {
        byField.computeIfAbsent(term.field(), _ -> new BytesRefHash()).add(term.bytes());
      }
      for (Map.Entry<String, BytesRefHash> entry : byField.entrySet()) {
        addRawTerms(doc, negativeDisjunctField(fields.map(entry.getKey()), i), entry.getValue());
        addRawTerms(doc, entry.getKey() + NOT_SUFFIX, entry.getValue());
      }
      addField(doc, negativeDisjunctMarkerField(fields, i), NEGATIVE_DISJUNCT_MARKER);
    }
    return doc;
  }

  private void addField(Document doc, String field, BytesRef term) {
    BytesRefHash terms = new BytesRefHash();
    terms.add(term);
    addField(doc, field, terms);
  }

  private void addField(Document doc, String field, BytesRefHash terms) {
    doc.add(
        new Field(
            field, new TermsEnumTokenStream(new BytesRefHashIterator(terms)), QUERYFIELDTYPE));
  }

  /**
   * Indexes exact bytes: TermsEnumTokenStream can merge invalid UTF-8 terms, which must not exclude
   */
  private static void addRawTerms(Document doc, String field, BytesRefHash terms) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < terms.size(); i++) {
      doc.add(new StringField(field, BytesRef.deepCopyOf(terms.get(i, scratch)), Field.Store.NO));
    }
  }

  /** The terms and negative disjuncts collected from one path through a {@link QueryTree} */
  private record CollectedTerms(
      Map<String, BytesRefHash> terms, List<Set<Term>> negativeDisjuncts) {}

  /** Collects the terms and negative disjuncts of the current path through a {@link QueryTree} */
  private CollectedTerms collectTermsAndNegativeDisjuncts(QueryTree querytree) {
    if (maxNegativeDisjuncts == 0) {
      return new CollectedTerms(collectTerms(querytree), List.of());
    }
    Map<String, BytesRefHash> fieldTerms = new HashMap<>();
    List<Set<Term>> negativeDisjuncts = new ArrayList<>();
    querytree.collectTerms(
        new QueryTree.TermCollector() {
          @Override
          public void accept(String field, BytesRef term) {
            fieldTerms.computeIfAbsent(field, _ -> new BytesRefHash()).add(term);
          }

          @Override
          public void negativeDisjunct(Set<Term> terms) {
            negativeDisjuncts.add(terms);
          }
        });
    return new CollectedTerms(fieldTerms, negativeDisjuncts);
  }

  /** Collects terms from a {@link QueryTree} and maps them per-field */
  protected Map<String, BytesRefHash> collectTerms(QueryTree querytree) {
    Map<String, BytesRefHash> fieldTerms = new HashMap<>();
    querytree.collectTerms(
        (field, term) -> {
          BytesRefHash tt = fieldTerms.computeIfAbsent(field, _ -> new BytesRefHash());
          tt.add(term);
        });
    return fieldTerms;
  }

  /** Implements a {@link BytesRefIterator} over a {@link BytesRefHash} */
  protected class BytesRefHashIterator implements BytesRefIterator {

    final BytesRef scratch = new BytesRef();
    final BytesRefHash terms;
    final int[] sortedTerms;
    int upto = -1;

    BytesRefHashIterator(BytesRefHash terms) {
      this.terms = terms;
      this.sortedTerms = terms.sort();
    }

    @Override
    public BytesRef next() {
      if (upto >= sortedTerms.length) return null;
      upto++;
      if (sortedTerms[upto] == -1) return null;
      this.terms.get(sortedTerms[upto], scratch);
      return scratch;
    }
  }
}
