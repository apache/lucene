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
package org.apache.lucene.analysis.compound.hyphenation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class TestPatternParser extends LuceneTestCase {

  /** Collects everything the parser hands to the consumer so we can inspect it. */
  private static class CollectingConsumer implements PatternConsumer {
    final StringBuilder collected = new StringBuilder();

    @Override
    public void addClass(String chargroup) {
      collected.append(chargroup);
    }

    @Override
    public void addException(String word, ArrayList<Object> hyphenatedword) {
      collected.append(word);
    }

    @Override
    public void addPattern(String pattern, String values) {
      collected.append(pattern);
    }
  }

  public void testExternalEntityIsNotExpanded() throws Exception {
    Path dir = createTempDir();
    Path secret = dir.resolve("secret.txt");
    String marker = "SUPERSECRETMARKER";
    Files.write(secret, marker.getBytes(StandardCharsets.UTF_8));

    Path evil = dir.resolve("evil.xml");
    Files.write(
        evil,
        ("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                + "<!DOCTYPE hyphenation-info [\n"
                + "  <!ENTITY xxe SYSTEM \""
                + secret.toUri()
                + "\">\n"
                + "]>\n"
                + "<hyphenation-info>\n"
                + "<classes>aA</classes>\n"
                + "<patterns>&xxe;</patterns>\n"
                + "</hyphenation-info>\n")
            .getBytes(StandardCharsets.UTF_8));

    CollectingConsumer consumer = new CollectingConsumer();
    PatternParser parser = new PatternParser(consumer);
    var e =
        expectThrows(
            IOException.class, () -> parser.parse(new InputSource(evil.toUri().toString())));
    assertTrue(e.getCause() instanceof SAXException);
    assertTrue(e.getMessage().contains("External Entity resolving unsupported"));
    assertTrue(e.getMessage().contains("secret.txt"));
  }
}
