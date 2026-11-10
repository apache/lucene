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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

final class JavascriptNestingDepthListener implements ParseTreeListener {
  private final String sourceText;
  private final int maxNestingDepth;

  private int depth = 0;

  public JavascriptNestingDepthListener(String sourceText, int maxNestingDepth) {
    super();
    this.sourceText = sourceText;
    this.maxNestingDepth = maxNestingDepth;
  }

  @Override
  public void visitTerminal(TerminalNode node) {}

  @Override
  public void visitErrorNode(ErrorNode node) {}

  @Override
  public void enterEveryRule(ParserRuleContext ctx) {
    depth++;
    if (depth > maxNestingDepth) {
      throw JavascriptCompiler.newWrappedParseException(
          "Invalid expression '"
              + sourceText
              + "': Nesting level too deep (>"
              + maxNestingDepth
              + ")",
          ctx.start != null ? ctx.start.getStartIndex() : -1);
    }
  }

  @Override
  public void exitEveryRule(ParserRuleContext ctx) {
    depth--;
  }
}
