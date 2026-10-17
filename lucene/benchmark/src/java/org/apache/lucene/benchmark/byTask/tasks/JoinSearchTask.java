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
package org.apache.lucene.benchmark.byTask.tasks;

import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.BRAND;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.BRANDS;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.COLOR;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.COLORS;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.FK;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.FK_NUM;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.ID;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.ID_NUM;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.PRODUCT;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.SIZE;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.SIZES;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.TYPE;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.value;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Counts products joined from skus, over the indices written by {@link JoinDocMaker} (see
 * conf/join-index.alg). Each query selects skus by a few colors or a few sizes, joins them to their
 * products and filters products by a few brands. Every instance draws the same query sequence from
 * <b>join.seed</b>, so instances with different algorithms are comparable.
 *
 * <p>The parameter is the algorithm:
 *
 * <ul>
 *   <li><b>term</b> - skus' {@link SortedDocValues} FK to products' PK terms (products, skus)
 *   <li><b>numeric</b> - skus' {@link org.apache.lucene.index.NumericDocValues} FK to products' PK
 *       points (products, skus)
 *   <li><b>globalOrd</b> - global ordinals over {@link SortedDocValues} FK (mixed)
 *   <li><b>block</b> - {@link ToParentBlockJoinQuery} (block)
 * </ul>
 *
 * <p>Other config properties: <b>join.dir</b>, see {@link JoinIndices}; <b>join.scoreMode</b> -
 * Default: None.
 */
public class JoinSearchTask extends PerfTask {

  private static final BitSetProducer PRODUCTS =
      new QueryBitSetProducer(new TermQuery(new Term(TYPE, PRODUCT)));

  private final Random random;
  private final ScoreMode scoreMode;
  private String algorithm;
  private SearcherManager fromManager;
  private SearcherManager toManager;
  private IndexSearcher from;
  private IndexSearcher to;
  private long totalHits;

  public JoinSearchTask(PerfRunData runData) {
    super(runData);
    random = new Random(runData.getConfig().get("join.seed", 42));
    scoreMode = ScoreMode.valueOf(runData.getConfig().get("join.scoreMode", "None"));
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (fromManager == null) {
      JoinIndices indices = JoinIndices.get(getRunData());
      String layout = algorithm.equals("globalOrd") ? "mixed" : algorithm;
      boolean split = algorithm.equals("term") || algorithm.equals("numeric");
      fromManager = indices.index(split ? "skus" : layout).manager();
      toManager = indices.index(split ? "products" : layout).manager();
    }
    from = fromManager.acquire();
    to = toManager.acquire();
  }

  @Override
  public int doLogic() throws Exception {
    boolean byColor = random.nextBoolean();
    Query skus = terms(byColor ? COLOR : SIZE, byColor ? COLORS : SIZES, 2 + random.nextInt(3));
    Query brands = terms(BRAND, BRANDS, 1 + random.nextInt(3));
    Query join =
        switch (algorithm) {
          case "term" -> JoinUtil.createJoinQuery(FK, false, ID, skus, from, scoreMode);
          case "numeric" ->
              JoinUtil.createJoinQuery(FK_NUM, false, ID_NUM, Long.class, skus, from, scoreMode);
          case "globalOrd" ->
              JoinUtil.createJoinQuery(
                  FK,
                  skus,
                  new TermQuery(new Term(TYPE, PRODUCT)),
                  from,
                  scoreMode,
                  ((JoinIndices.JoinSearcher) from).ordinalMap);
          default -> new ToParentBlockJoinQuery(skus, PRODUCTS, scoreMode);
        };
    totalHits +=
        to.count(
            new BooleanQuery.Builder().add(join, Occur.MUST).add(brands, Occur.FILTER).build());
    return 1;
  }

  @Override
  public void tearDown() throws Exception {
    fromManager.release(from);
    toManager.release(to);
    from = to = null;
    super.tearDown();
  }

  private Query terms(String field, int vocabulary, int count) {
    TreeSet<Integer> picked = new TreeSet<>();
    while (picked.size() < count) {
      picked.add(random.nextInt(vocabulary));
    }
    List<BytesRef> terms = new ArrayList<>();
    for (int n : picked) {
      terms.add(new BytesRef(value(field, n)));
    }
    return new TermInSetQuery(field, terms);
  }

  /** Sum of hit counts over all executed queries. */
  public long getTotalHits() {
    return totalHits;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    if (!List.of("term", "numeric", "globalOrd", "block").contains(params)) {
      throw new IllegalArgumentException("Unknown join algorithm: " + params);
    }
    algorithm = params;
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
