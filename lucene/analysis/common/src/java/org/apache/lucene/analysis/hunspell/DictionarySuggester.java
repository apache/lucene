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
package org.apache.lucene.analysis.hunspell;

import java.util.List;
import java.util.Set;

/** Generates suggestions by traversing dictionary entries. */
@FunctionalInterface
public interface DictionarySuggester {

  /**
   * @param hunspell spell checker configured for suggestion generation
   * @param suggestibleEntryCache optional cache from {@link Suggester#withSuggestibleEntryCache()}
   *     or {@code null}
   * @param lowerCase the lower-case misspelled word
   * @param wordCase case of the misspelled word before lower-casing
   * @param suggestions suggestions already found by earlier phases
   * @return raw suggestions to add
   */
  List<String> suggest(
      Hunspell hunspell,
      SuggestibleEntryCache suggestibleEntryCache,
      String lowerCase,
      WordCase wordCase,
      Set<Suggestion> suggestions);
}
