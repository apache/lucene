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
package org.apache.lucene.queryparser.ext;

import org.apache.lucene.queryparser.classic.QueryParser;

/**
 * {@link ExtensionQuery} holds all query components extracted from the original query string like
 * the query field and the extension query string.
 *
 * @param topLevelParser the top level parser
 * @param field the query field
 * @param rawQueryString the raw extension query string
 * @see Extensions
 * @see ExtendableQueryParser
 * @see ParserExtension
 */
public record ExtensionQuery(QueryParser topLevelParser, String field, String rawQueryString) {}
