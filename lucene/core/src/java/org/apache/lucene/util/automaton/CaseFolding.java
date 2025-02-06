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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.IOUtils;

/**
 * A utility class that allows downstream code to be consistent with how Lucene handles case folding
 */
public class CaseFolding {

  private static final Map<Integer, int[]> unstableUnicodeCharacters;

  static {
    try {
      unstableUnicodeCharacters = CaseFolding.loadMapping();
    } catch (IOException ex) {
      // default set should always be present as it is part of the distribution (JAR)
      throw new UncheckedIOException("Unable to load case folding mapping", ex);
    }
  }

  /**
   * generates the set of codepoints which represent the given codepoint that are case-insensitive
   * matches within the Unicode table, which may not always be intuitive for instance Σ, σ, ς do all
   * fold together and so would match one another
   *
   * @param codepoint the codepoint for the character to case fold
   * @return an array of characters as codepoints that should match the given codepoint in a
   *     case-insensitive context or null if no alternates exist
   */
  public int[] fold(int codepoint) {
    return unstableUnicodeCharacters.get(codepoint);
  }

  static Map<Integer, int[]> loadMapping() throws IOException {
    Map<Integer, int[]> caseFolding = new HashMap<>();

    InputStream stream =
        IOUtils.requireResourceNonNull(
            RegExp.class.getResourceAsStream("casefolding.txt"), "casefolding.txt");
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] splits = line.split(";");
        int codepoint = Integer.decode(splits[0]);
        String[] alternateStrings = splits[1].split(",");
        int[] alternates = new int[alternateStrings.length + 1];
        for (int i = 0; i < alternateStrings.length; i++) {
          alternates[i] = Integer.decode(alternateStrings[i].strip());
        }
        alternates[alternateStrings.length] = codepoint;
        caseFolding.put(codepoint, alternates);
      }
    }

    return caseFolding;
  }
}
