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

package org.apache.lucene.luke.app.web;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.lucene.luke.util.HttpUtil.parseQueryString;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luke.models.search.QueryParserConfig;
import org.apache.lucene.luke.models.search.Search;
import org.apache.lucene.luke.models.search.SearchResults;
import org.apache.lucene.luke.models.search.SimilarityConfig;
import org.apache.lucene.luke.util.JsonUtil;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.search.Query;

final class SearchHandler extends HandlerBase {

  private static final Logger LOG = LoggerFactory.getLogger(SearchHandler.class);

  private final Search searcher;

  SearchHandler(Search searcher) {
    this.searcher = searcher;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String queryString = exchange.getRequestURI().getRawQuery(); // only GET supported
    if (queryString == null) {
      handleInitialData(exchange);
      return;
    }
    LOG.info("GET /search?" + queryString);
    Map<String, String> params = parseQueryString(queryString);
    String q = params.getOrDefault("q", "");
    if (q.isEmpty()) {
      sendResponse(exchange, "{}".getBytes(UTF_8), MIME_JSON);
      return;
    }
    try {
      String scope = params.getOrDefault("scope", "");
      String operator = params.getOrDefault("operator", "OR");
      Query query = parse(q, scope, operator, false);
      LOG.info("/search query=" + query);
      SearchResults results =
          searcher.search(
              query,
              new SimilarityConfig.Builder().build(),
              null,
              Set.of("title", "asin", "description", "bullet_point"),
              20,
              false);
      LOG.info("/search returned " + results.size() + " results");
      sendResponse(exchange, serializeResponse(query, results), MIME_JSON);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.toString());
      sendException(exchange, e);
    }
  }

  public static class Response {
    public int start;
    public long totalHits;
    public String parsedQuery;
    public List<SearchResults.Doc> hits = new ArrayList<>();
  }

  private byte[] serializeResponse(Query query, SearchResults results) {
    Response response = new Response();
    response.start = results.getOffset();
    response.totalHits = results.getTotalHits().value;
    response.hits = results.getHits();
    response.parsedQuery = query.toString();
    String responseText = JsonUtil.asJson(response);
    return responseText.getBytes(UTF_8);
  }

  private Query parse(String expr, String scope, String operatorString, boolean rewrite) {
    String df = "text";
    QueryParserConfig.Operator operator;
    switch (operatorString) {
      default:
      case "or":
        operator = QueryParserConfig.Operator.OR;
        break;
      case "and":
        operator = QueryParserConfig.Operator.AND;
        break;
    }
    QueryParserConfig config = new QueryParserConfig.Builder().defaultOperator(operator).build();
    Analyzer analyzer = new StandardAnalyzer();
    return searcher.parseQuery(expr, df, analyzer, config, rewrite);
  }

  private void handleInitialData(HttpExchange exchange) throws IOException {
    LOG.info("GET /search");
    sendResponse(
        exchange, JsonUtil.asJson(Map.of("field_infos", searcher.getFieldInfos())), MIME_JSON);
  }
}
