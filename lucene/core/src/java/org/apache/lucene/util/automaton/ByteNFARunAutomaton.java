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
 * Automaton for matching a byte array against a non-deterministic automaton. Note: the current
 * implementation is NOT thread-safe!
 *
 * @lucene.internal
 */
public class ByteNFARunAutomaton extends NFARunAutomaton implements ByteRunnable {
  /** Construct from an automaton, with alphabet size of 2^8 (range of unsigned byte). */
  public ByteNFARunAutomaton(Automaton automaton) {
    this(automaton, 0xff);
  }

  /** Construct from an automaton and alphabet size. */
  public ByteNFARunAutomaton(Automaton automaton, int alphabetSize) {
    super(automaton, alphabetSize);
  }

  /**
   * Run through a given codepoint array, return accepted or not, should only be used in test
   *
   * @param s String represented by an int array
   * @return accept or not
   */
  boolean run(int[] s) {
    int p = 0;
    for (int c : s) {
      p = step(p, c);
      if (p == -1) return false;
    }
    return isAccept(p);
  }
}
