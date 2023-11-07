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

/** A runnable automaton accepting byte array as input */
public interface ByteRunnable {

  /**
   * Returns the state obtained by reading the given char from the given state. Returns -1 if not
   * obtaining any such state.
   *
   * @param state the last state
   * @param c the input codepoint
   * @return the next state, -1 if no such transaction
   */
  int step(int state, int c);

  /**
   * Returns acceptance status for given state.
   *
   * @param state the state
   * @return whether the state is accepted
   */
  boolean isAccept(int state);

  /**
   * Returns number of states this automaton has, note this may not be an accurate number in case of
   * NFA
   *
   * @return number of states
   */
  int getSize();

  /** Returns true if the given byte array is accepted by this automaton */
  default boolean run(byte[] s, int offset, int length) {
    int p = 0;
    int l = offset + length;
    for (int i = offset; i < l; i++) {
      p = step(p, s[i] & 0xFF);
      if (p == -1) return false;
    }
    return isAccept(p);
  }
}
