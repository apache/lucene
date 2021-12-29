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
 * Support for testing analysis components.
 *
 * <p>The main classes of interest are:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.tests.analysis.BaseTokenStreamTestCase}: Highly recommended to use
 *       its helper methods, (especially in conjunction with {@link
 *       org.apache.lucene.tests.analysis.MockAnalyzer} or {@link
 *       org.apache.lucene.tests.analysis.MockTokenizer}), as it contains many assertions and checks
 *       to catch bugs.
 *   <li>{@link org.apache.lucene.tests.analysis.MockTokenizer}: Tokenizer for testing. Tokenizer
 *       that serves as a replacement for WHITESPACE, SIMPLE, and KEYWORD tokenizers. If you are
 *       writing a component such as a {@link org.apache.lucene.analysis.TokenFilter}, it's a great
 *       idea to test it wrapping this tokenizer instead for extra checks.
 *   <li>{@link org.apache.lucene.tests.analysis.MockAnalyzer}: Analyzer for testing. Analyzer that
 *       uses MockTokenizer for additional verification. If you are testing a custom component such
 *       as a queryparser or analyzer-wrapper that consumes analysis streams, it's a great idea to
 *       test it with this analyzer instead.
 * </ul>
 */
package org.apache.lucene.tests.analysis;
