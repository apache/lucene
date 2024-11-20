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

/**
 * Flexible query parser is a modular, extensible framework for implementing Lucene query parsers.
 * In the flexible query parser model, query parsing takes three steps: syntax parsing, processing
 * (query semantics) and building (conversion to a Lucene {@link org.apache.lucene.search.Query}).
 *
 * <p>The flexible query parser module provides not just the framework but also the {@linkplain
 * org.apache.lucene.queryparser.flexible.standard.StandardQueryParser} - the default implementation
 * of a fully fledged query parser that supports most of the classic query parser's syntax but also
 * adds support for interval functions, min-should-match operator on Boolean groups and many hooks
 * for customization of how the parser behaves at runtime.
 *
 * <p>The flexible query parser is divided in two packages:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.queryparser.flexible.core}: contains the query parser API classes,
 *       which should be extended by custom query parser implementations.
 *   <li>{@link org.apache.lucene.queryparser.flexible.standard}: contains an example Lucene query
 *       parser implementation built on top of the flexible query parser API.
 * </ul>
 *
 * <h2>Features</h2>
 *
 * <ol>
 *   <li>full support for Boolean expressions, including groups
 *   <li>{@linkplain org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser syntax parsers}
 *       - support for arbitrary syntax parsers, that can be converted into {@link
 *       org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} trees.
 *   <li>{@linkplain org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor query
 *       node processors} - optimize, validate, rewrite the {@link
 *       org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} trees
 *   <li>{@linkplain
 *       org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline processor
 *       pipelines} - select your favorite query processors and build a pipeline to implement the
 *       features you need.
 *   <li>{@linkplain org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler query
 *       configuration handlers}
 *   <li>{@linkplain org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder query
 *       builders} - convert {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}
 *       trees into Lucene {@link org.apache.lucene.search.Query} instances.
 * </ol>
 *
 * <h2>Design</h2>
 *
 * <p>The flexible query parser was designed to have a very generic architecture, so that it can be
 * easily used for different products with varying query syntax needs.
 *
 * <p>The query parser has three layers and its core is what we call the {@linkplain
 * org.apache.lucene.queryparser.flexible.core.nodes.QueryNode query node tree}. It is a tree of
 * objects that represent the syntax of the original query, for example, for 'a AND b' the tree
 * could look like this:
 *
 * <pre>
 *       AND
 *      /   \
 *     A     B
 * </pre>
 *
 * <p>The three flexible query parser layers are:
 *
 * <dl>
 *   <dt>{@link org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser}
 *   <dd>This layer is the text parsing layer which simply transforms the query text string into a
 *       {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} tree. Every text parser
 *       must implement the interface {@link
 *       org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser}. The default
 *       implementation is {@link
 *       org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser}.
 *   <dt>{@link org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor}
 *   <dd>The query node processor does most of the work: it contains a chain of {@linkplain
 *       org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor query node
 *       processors}. Each processor can walk the tree and modify nodes or even the tree's
 *       structure. This allows for query optimization before the node tree is converted to an
 *       actual query.
 *   <dt>{@link org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder}
 *   <dd>The third layer is a configurable map of builders, which map {@linkplain
 *       org.apache.lucene.queryparser.flexible.core.nodes.QueryNode query nodes} to their adapters
 *       that convert each node into a {@link org.apache.lucene.search.Query}.
 * </dl>
 */
package org.apache.lucene.queryparser.flexible;
