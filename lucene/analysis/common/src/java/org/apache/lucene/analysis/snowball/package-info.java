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
 * {@link org.apache.lucene.analysis.TokenFilter} and {@link org.apache.lucene.analysis.Analyzer}
 * implementations that use a modified version of <a href="https://snowballstem.org/">Snowball
 * stemmers</a>. See <a href="https://snowballstem.org/">Snowball project page</a> for more
 * information about the original algorithms used.
 *
 * <p>Lucene snowball classes require a few patches to the original Snowball source tree to generate
 * more efficient code.
 *
 * <p>Refer to {@code gradle/generation/snowball*} and {@code help/regeneration.txt} files in Lucene
 * source code for instructions on how code regeneration from Snowball sources works, what
 * modifications are applied and what is required to regenerate snowball analyzers from scratch.
 *
 * <p><b>IMPORTANT NOTICE ON BACKWARDS COMPATIBILITY!</b>
 *
 * <p>An index created using the Snowball module in one Lucene version may not be compatible with an
 * index created with another Lucene version. The token stream will vary depending on the changes in
 * snowball stemmer definitions.
 */
package org.apache.lucene.analysis.snowball;
