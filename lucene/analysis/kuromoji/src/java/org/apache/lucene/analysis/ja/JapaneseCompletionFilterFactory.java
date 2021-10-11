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
package org.apache.lucene.analysis.ja;

import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link JapaneseCompletionFilter}.
 *
 * <p>Supported attributes:
 *
 * <ul>
 *   <li>mode: Completion mode. see {@link JapaneseCompletionFilter.Mode}
 * </ul>
 *
 * @lucene.spi {@value #NAME}
 */
public class JapaneseCompletionFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "japaneseCompletion";

  private static final String MODE_PARAM = "mode";
  private final JapaneseCompletionFilter.Mode mode;

  /** Creates a new {@code JapaneseCompletionFilterFactory} */
  public JapaneseCompletionFilterFactory(Map<String, String> args) {
    super(args);
    mode =
        JapaneseCompletionFilter.Mode.valueOf(
            get(
                args,
                MODE_PARAM,
                JapaneseCompletionFilter.DEFAULT_MODE.name().toUpperCase(Locale.ROOT)));
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public JapaneseCompletionFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new JapaneseCompletionFilter(input, mode);
  }
}
