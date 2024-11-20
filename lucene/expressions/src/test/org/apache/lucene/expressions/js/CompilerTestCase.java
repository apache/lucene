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

import java.lang.invoke.MethodHandle;
import java.text.ParseException;
import java.util.Map;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Base class for testing JS compiler */
public abstract class CompilerTestCase extends LuceneTestCase {

  /** compiles expression for sourceText with default functions list */
  protected Expression compile(String sourceText) throws ParseException {
    return compile(sourceText, JavascriptCompiler.DEFAULT_FUNCTIONS);
  }

  /** compiles expression for sourceText with custom functions list */
  protected Expression compile(String sourceText, Map<String, MethodHandle> functions)
      throws ParseException {
    return JavascriptCompiler.compile(sourceText, functions, true);
  }
}
