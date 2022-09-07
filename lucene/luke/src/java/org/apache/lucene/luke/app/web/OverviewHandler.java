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

import static org.apache.lucene.luke.util.HttpUtil.parseQueryString;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.models.overview.Overview;
import org.apache.lucene.luke.models.overview.OverviewFactory;
import org.apache.lucene.luke.util.JsonUtil;
import org.apache.lucene.luke.util.LoggerFactory;

final class OverviewHandler extends HandlerBase {

  private static Logger LOG = LoggerFactory.getLogger(OverviewHandler.class);

  private final Overview overview;

  OverviewHandler() {
    LukeState state = IndexHandler.getInstance().getState();
    overview = new OverviewFactory().newInstance(state.getIndexReader(), state.getIndexPath());
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    switch (exchange.getRequestURI().getPath()) {
      case "/terms":
        handleTerms(exchange);
        break;
      default:
      case "/overview":
        handleOverview(exchange);
        break;
    }
  }

  private void handleOverview(HttpExchange exchange) throws IOException {
    LOG.info("overview");
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("Index path", overview.getIndexPath());
    summary.put("Number of fields", overview.getNumFields());
    summary.put("Number of documents", overview.getNumDocuments());
    summary.put("Number of terms", overview.getNumTerms());
    summary.put("Number of deletions", overview.getNumDeletedDocs());
    summary.put("Index version", overview.getIndexVersion().get());
    summary.put("Index format", overview.getIndexFormat().get());
    summary.put("Directory implementation", overview.getDirImpl().get());
    summary.put("Currently opened commit point", overview.getCommitDescription().get());
    summary.put("Current commit user data", overview.getCommitUserData().get());
    Map<String, ?> termFields = overview.getSortedTermCounts(null);
    Map<?, ?> response =
        Map.of(
            "summary", summary,
            "termFields", termFields,
            "fieldDocCount", getDocCounts(termFields.keySet()));
    sendResponse(exchange, JsonUtil.asJson(response), MIME_JSON);
  }

  private Map<?, ?> getDocCounts(Set<String> fields) throws IOException {
    // TODO: make a better model that includes more field info
    Map<String, Integer> coverage = new HashMap<>();
    IndexReader reader = IndexHandler.getInstance().getState().getIndexReader();
    for (String field : fields) {
      Terms terms = MultiTerms.getTerms(reader, field);
      coverage.put(field, terms.getDocCount());
    }
    return coverage;
  }

  private void handleTerms(HttpExchange exchange) throws IOException {
    String queryString = exchange.getRequestURI().getRawQuery();
    LOG.info("terms?" + queryString);
    try {
      Map<String, String> params = parseQueryString(queryString);
      String field = params.getOrDefault("field", "");
      int numTerms = Integer.parseInt(params.getOrDefault("size", "50"));
      sendResponse(exchange, JsonUtil.asJson(overview.getTopTerms(field, numTerms)), MIME_JSON);
    } catch (Exception e) {
      sendException(exchange, e);
    }
  }
}
