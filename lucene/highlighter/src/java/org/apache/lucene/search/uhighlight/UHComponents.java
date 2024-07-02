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

package org.apache.lucene.search.uhighlight;

import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * A parameter object to hold the components a {@link FieldOffsetStrategy} needs.
 *
 * @param terms Query: all terms we extracted (some may be position sensitive)
 * @param phraseHelper Query: position-sensitive information
 * @param automata Query: wildcards (i.e. multi-term query), not position sensitive
 * @param hasUnrecognizedQueryPart Query: if part of the query (other than the extracted terms /
 *     automata) is a leaf we don't know
 * @lucene.internal
 */
public record UHComponents(
    String field,
    Predicate<String> fieldMatcher,
    Query query,
    BytesRef[] terms,
    PhraseHelper phraseHelper,
    LabelledCharArrayMatcher[] automata,
    boolean hasUnrecognizedQueryPart,
    Set<UnifiedHighlighter.HighlightFlag> highlightFlags) {}
