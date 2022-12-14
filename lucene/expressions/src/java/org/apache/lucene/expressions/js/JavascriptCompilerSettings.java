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
package org.apache.lucene.expressions.js;

/** Settings for expression compiler for javascript expressions. */
final class JavascriptCompilerSettings {
  static final JavascriptCompilerSettings DEFAULT = new JavascriptCompilerSettings();

  /**
   * Whether to throw exception on ambiguity or other internal parsing issues. This option makes
   * things slower too, it is only for debugging.
   */
  private boolean picky = false;

  /**
   * Returns true if the compiler should be picky. This means it runs slower and enables additional
   * runtime checks, throwing an exception if there are ambiguities in the grammar or other low
   * level parsing problems.
   */
  public boolean isPicky() {
    return picky;
  }

  /**
   * Set to true if compilation should be picky.
   *
   * @see #isPicky
   */
  public void setPicky(boolean picky) {
    this.picky = picky;
  }

  @Override
  public String toString() {
    return "[picky=" + picky + "]";
  }
}
