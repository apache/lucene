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

/** Automaton representation for matching UTF-8 byte[]. */
public class ByteRunAutomaton extends RunAutomaton implements ByteRunnable {

  /**
   * Converts incoming automaton to byte-based (UTF32ToUTF8) first
   *
   * @throws IllegalArgumentException if the automaton is not deterministic
   */
  public ByteRunAutomaton(Automaton a) {
    this(a, false);
  }

  /**
   * expert: if isBinary is true, the input is already byte-based
   *
   * @throws IllegalArgumentException if the automaton is not deterministic
   */
  public ByteRunAutomaton(Automaton a, boolean isBinary) {
    super(isBinary ? a : convert(a), 256);
  }

  static Automaton convert(Automaton a) {
    if (!a.isDeterministic()) {
      throw new IllegalArgumentException("Automaton must be deterministic");
    }
    // we checked the input is a DFA, according to mike this determinization is contained :)
    return Operations.determinize(new UTF32ToUTF8().convert(a), Integer.MAX_VALUE);
  }
}
