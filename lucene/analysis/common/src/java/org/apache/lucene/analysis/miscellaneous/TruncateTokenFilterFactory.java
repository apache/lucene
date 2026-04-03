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

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter}.
 *
 * <p>Fixed prefix truncation, as a stemming method, produces good results on Turkish language. It
 * is reported that F5, using first 5 characters, produced best results in <a
 * href="https://doi.org/10.1002/asi.20750">Information Retrieval on Turkish Texts</a>
 *
 * <p>Since Lucene 10.5, the filter correctly handles codepoints and truncates after {@code
 * truncateAfterCodePoints} codepoints, no longer producing incomplete surrogate pairs. For
 * backwards compatibility the old {@code prefixLength} is still supported and truncates after
 * utf-16/java characters. If no argument is given it uses a prefix length of 5 java characters and
 * does not respect codepoints. In case you change that behaviour, reindex may be required if your
 * documents contain surrogate pairs (like emojis).
 *
 * <p>The following type is recommended for "<i>diacritics-insensitive search</i>" for Turkish:
 *
 * <pre><code class="language-xml">
 * &lt;fieldType name="text_tr_ascii_f5" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ApostropheFilterFactory"/&gt;
 *     &lt;filter class="solr.TurkishLowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ASCIIFoldingFilterFactory" preserveOriginal="true"/&gt;
 *     &lt;filter class="solr.KeywordRepeatFilterFactory"/&gt;
 *     &lt;filter class="solr.TruncateTokenFilterFactory" truncateAfterCodePoints="5"/&gt;
 *     &lt;filter class="solr.RemoveDuplicatesTokenFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</code></pre>
 *
 * @since 4.8.0
 * @lucene.spi {@value #NAME}
 */
public class TruncateTokenFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "truncate";

  @Deprecated public static final String PREFIX_LENGTH_KEY = "prefixLength";
  public static final String TRUNCATE_AFTER_CODEPOINTS_KEY = "truncateAfterCodePoints";
  public static final String TRUNCATE_AFTER_CHARS_KEY = "truncateAfterChars";

  private static final Map<String, BiFunction<TokenStream, Integer, TruncateTokenFilter>>
      PARAM_MAPPING =
          Map.of(
              TRUNCATE_AFTER_CODEPOINTS_KEY, TruncateTokenFilter::truncateAfterCodePoints,
              TRUNCATE_AFTER_CHARS_KEY, TruncateTokenFilter::truncateAfterChars,
              PREFIX_LENGTH_KEY, TruncateTokenFilter::truncateAfterChars);

  private final int truncateAfter;
  private final BiFunction<TokenStream, Integer, TruncateTokenFilter> factory;

  public TruncateTokenFilterFactory(Map<String, String> args) {
    super(args);
    var avail = PARAM_MAPPING.keySet().stream().filter(args::containsKey).toList();
    if (avail.size() > 1) {
      throw new IllegalArgumentException(
          "Can only give one of the following parameters: " + PARAM_MAPPING.keySet());
    }
    String param = avail.stream().findFirst().orElse(PREFIX_LENGTH_KEY);
    this.truncateAfter = getInt(args, param, 5);
    this.factory = PARAM_MAPPING.get(param);
    if (truncateAfter < 1) {
      throw new IllegalArgumentException(
          param + " parameter must be a positive number: " + truncateAfter);
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameter(s): " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public TruncateTokenFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return factory.apply(input, truncateAfter);
  }
}
