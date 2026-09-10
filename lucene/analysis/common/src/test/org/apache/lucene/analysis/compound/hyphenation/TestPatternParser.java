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
import java.io.StringReader;
import java.util.Locale;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class TestPatternParser extends LuceneTestCase {

  public void testExternalEntityIsNotExpanded() throws Exception {
    String externalRef = this.getClass().getResource("../compoundDictionary.txt").toExternalForm();
    String evil =
        String.format(
            Locale.ROOT,
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <!DOCTYPE hyphenation-info [
              <!ENTITY xxe SYSTEM "%s">
            ]>
            <hyphenation-info>
              <classes>aA</classes>
              <patterns>&xxe;</patterns>
            </hyphenation-info>
            """,
            externalRef);

    PatternParser parser = new PatternParser(new HyphenationTree());
    var e =
        expectThrows(
            IOException.class, () -> parser.parse(new InputSource(new StringReader(evil))));
    assertTrue(e.getCause() instanceof SAXException);
    assertTrue(e.getMessage().contains("External Entity resolving unsupported"));
    assertTrue(e.getMessage().contains(externalRef));
  }
}
