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

/** Interface accessing the transitions of an automaton */
public interface TransitionAccessor {

  /**
   * Initialize the provided Transition to iterate through all transitions leaving the specified
   * state. You must call {@link #getNextTransition} to get each transition. Returns the number of
   * transitions leaving this state.
   */
  int initTransition(int state, Transition t);

  /** Iterate to the next transition after the provided one */
  void getNextTransition(Transition t);

  /** How many transitions this state has. */
  int getNumTransitions(int state);

  /**
   * Fill the provided {@link Transition} with the index'th transition leaving the specified state.
   */
  void getTransition(int state, int index, Transition t);
}
