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
package org.apache.lucene.queryparser.flexible.standard.processors;

import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.IntervalQueryNode;

/**
 * This processor makes sure that {@link ConfigurationKeys#ANALYZER} is defined in the {@link
 * QueryConfigHandler} and injects this analyzer into {@link
 * org.apache.lucene.queryparser.flexible.standard.nodes.IntervalQueryNode}s.
 *
 * @see ConfigurationKeys#ANALYZER
 */
public class IntervalQueryNodeProcessor extends QueryNodeProcessorImpl {
  private Analyzer analyzer;

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    this.analyzer = getQueryConfigHandler().get(ConfigurationKeys.ANALYZER);
    return super.process(queryTree);
  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    if (node instanceof IntervalQueryNode) {
      var intervalQueryNode = (IntervalQueryNode) node;
      if (this.analyzer == null) {
        throw new QueryNodeException(
            new MessageImpl(QueryParserMessages.ANALYZER_REQUIRED, intervalQueryNode.toString()));
      }
      intervalQueryNode.setAnalyzer(this.analyzer);
    }
    return node;
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {
    return children;
  }
}
