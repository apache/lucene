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

import java.io.IOException;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.search.DoubleValues;

/** Some sanity checks with API as those are not detected by {@link JavascriptCompiler} */
public class TestAPISanity extends CompilerTestCase {

  // if this class does not compile, the Exception types of evaluate and DoubleValues do not match.
  @SuppressWarnings("unused")
  private static class SanityExpression extends Expression {

    public SanityExpression(String sourceText, String[] variables) {
      super(sourceText, variables);
    }

    @Override
    public double evaluate(DoubleValues[] functionValues) throws IOException {
      return functionValues[0].doubleValue();
    }
  }

  public void testBytecodeExceptions() throws Exception {
    Expression expr = compile("1");
    assertArrayEquals(
        Expression.class.getDeclaredMethod("evaluate", DoubleValues[].class).getExceptionTypes(),
        expr.getClass().getDeclaredMethod("evaluate", DoubleValues[].class).getExceptionTypes());
  }
}
