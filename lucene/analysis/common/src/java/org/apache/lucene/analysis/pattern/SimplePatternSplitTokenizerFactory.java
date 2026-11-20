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

package org.apache.lucene.analysis.pattern;

import java.util.Map;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Factory for {@link SimplePatternSplitTokenizer}, for producing tokens by splitting according to
 * the provided regexp.
 *
 * <p>This tokenizer uses Lucene {@link RegExp} pattern matching to construct distinct tokens for
 * the input stream. The syntax is more limited than {@link PatternTokenizer}, but the tokenization
 * is quite a bit faster. It takes one argument: <br>
 *
 * <ul>
 *   <li>"pattern" (required) is the regular expression, according to the syntax described at {@link
 *       RegExp}
 * </ul>
 *
 * <p>The pattern matches the characters that should split tokens, like {@code String.split}, and
 * the matching is greedy such that the longest token separator matching at a given point is
 * matched. Empty tokens are never created.
 *
 * <p>For example, to match tokens delimited by simple whitespace characters:
 *
 * <pre><code class="language-xml">
 * &lt;fieldType name="text_ptn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.SimplePatternSplitTokenizerFactory" pattern="[ \t\r\n]+"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</code></pre>
 *
 * @lucene.experimental
 * @see SimplePatternSplitTokenizer
 * @since 6.5.0
 * @lucene.spi {@value #NAME}
 */
public class SimplePatternSplitTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "simplePatternSplit";

  public static final String PATTERN = "pattern";
  private final Automaton automaton;

  /** Creates a new SimpleSplitPatternTokenizerFactory */
  public SimplePatternSplitTokenizerFactory(Map<String, String> args) {
    super(args);
    automaton = new RegExp(require(args, PATTERN)).toAutomaton();
    if (args.isEmpty() == false) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public SimplePatternSplitTokenizerFactory() {
    throw defaultCtorException();
  }

  @Override
  public SimplePatternSplitTokenizer create(final AttributeFactory factory) {
    return new SimplePatternSplitTokenizer(factory, automaton);
  }
}
