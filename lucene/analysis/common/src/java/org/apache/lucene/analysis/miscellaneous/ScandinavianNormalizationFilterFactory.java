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
package org.apache.lucene.analysis.miscellaneous;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter.Foldings;

/**
 * Factory for {@link org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter}.
 * Use parameter "foldings" to control which of the five available foldings aa, ao, ae, oe and oo
 * to apply. The input is a comma separate list, e.g. for Norwegian it makes sense with <code>foldings="ae,aa,ao"</code>.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_scandnorm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ScandinavianNormalizationFilterFactory" foldings="ae,aa,ao"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 4.4.0
 * @lucene.spi {@value #NAME}
 */
public class ScandinavianNormalizationFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "scandinavianNormalization";
  private static final String FOLDINGS_KEY = "foldings";
  private final Set<Foldings> foldings;

  public ScandinavianNormalizationFilterFactory(Map<String, String> args) {
    super(args);
    String foldingsStr = get(args, FOLDINGS_KEY);
    if (foldingsStr != null) {
      foldings = Arrays.stream(foldingsStr.split(", *"))
          .map(s -> Foldings.valueOf(s.toUpperCase(Locale.ROOT))).collect(Collectors.toSet());
    } else {
      foldings = Collections.emptySet();
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public ScandinavianNormalizationFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public ScandinavianNormalizationFilter create(TokenStream input) {
    if (foldings.isEmpty()) {
      return new ScandinavianNormalizationFilter(input);
    } else {
      return new ScandinavianNormalizationFilter(input, foldings);
    }
  }

  @Override
  public TokenStream normalize(TokenStream input) {
    return create(input);
  }

}
