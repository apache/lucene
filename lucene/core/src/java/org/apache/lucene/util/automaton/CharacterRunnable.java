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

/** A runnable automaton accepting character sequences as input. */
public interface CharacterRunnable extends Runnable {
  /** Returns true if the given character sequence is accepted by this automaton. */
  default boolean run(CharSequence s) {
    int state = 0;
    for (int start = 0, end = s.length(), codePoint;
        start < end;
        start += Character.charCount(codePoint)) {
      state = step(state, codePoint = Character.codePointAt(s, start));
      if (state == -1) return false;
    }
    return isAccept(state);
  }

  /** Returns true if the given char[] is accepted by this automaton. */
  default boolean run(char[] s, int offset, int length) {
    int state = 0;
    for (int start = offset, end = offset + length, codePoint;
        start < end;
        start += Character.charCount(codePoint)) {
      state = step(state, codePoint = Character.codePointAt(s, start, end));
      if (state == -1) return false;
    }
    return isAccept(state);
  }
}
