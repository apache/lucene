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

package org.apache.lucene.util.automaton;

/**
 * Running automaton based on NFA or DFA approach
 *
 * @lucene.experimental
 */
public enum RunAutomatonMode {
  /**
   * Determinize the automaton lazily on-demand as terms are intersected. This option saves the
   * up-front determinize cost, and can handle some RegExps that DFA cannot, but intersection will
   * be a bit slower.
   *
   * @lucene.experimental
   */
  NFA,
  /**
   * Fully determinize the automaton up-front for fast term intersection. Some RegExps may fail to
   * determinize, throwing TooComplexToDeterminizeException. But if they do not, intersection is
   * fast.
   *
   * @lucene.experimental
   */
  DFA
}
