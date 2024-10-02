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

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestDefaultPassageFormatter extends LuceneTestCase {
  public void testBasic() throws Exception {
    String text =
        "Test customization & <div class=\"xy\">&quot;escaping&quot;</div> of this very formatter. Unrelated part. It's not very N/A!";
    // fabricate passages with matches to format
    Passage[] passages = new Passage[2];
    passages[0] = new Passage();
    passages[0].setStartOffset(0);
    passages[0].setEndOffset(text.indexOf('.') + 1);
    passages[0].addMatch(text.indexOf("very"), text.indexOf("very") + 4, null, 2);
    passages[1] = new Passage();
    passages[1].setStartOffset(text.indexOf('.', passages[0].getEndOffset() + 1) + 2);
    passages[1].setEndOffset(text.length());
    passages[1].addMatch(
        text.indexOf("very", passages[0].getEndOffset()),
        text.indexOf("very", passages[0].getEndOffset()) + 4,
        null,
        2);

    // test default
    DefaultPassageFormatter formatter = new DefaultPassageFormatter();
    assertEquals(
        "Test customization & <div class=\"xy\">&quot;escaping&quot;</div> of this <b>very</b> formatter."
            + "... It's not <b>very</b> N/A!",
        formatter.format(passages, text));

    // test customization and encoding
    formatter = new DefaultPassageFormatter("<u>", "</u>", "\u2026 ", true);
    assertEquals(
        "Test customization &amp; &lt;div class=&quot;xy&quot;&gt;&amp;quot;escaping&amp;quot;"
            + "&lt;&#x2F;div&gt; of this <u>very</u> formatter.\u2026 It&#x27;s not <u>very</u> N&#x2F;A!",
        formatter.format(passages, text));
  }

  public void testOverlappingPassages() throws Exception {
    String content = "Yin yang loooooooooong, yin gap yang yong";
    Passage[] passages = new Passage[1];
    passages[0] = new Passage();
    passages[0].setStartOffset(0);
    passages[0].setEndOffset(41);
    passages[0].setScore(5.93812f);
    passages[0].setScore(5.93812f);
    passages[0].addMatch(0, 3, new BytesRef("yin"), 1);
    passages[0].addMatch(0, 22, new BytesRef("yin yang loooooooooooong"), 1);
    passages[0].addMatch(4, 8, new BytesRef("yang"), 1);
    passages[0].addMatch(9, 22, new BytesRef("loooooooooong"), 1);
    passages[0].addMatch(24, 27, new BytesRef("yin"), 1);
    passages[0].addMatch(32, 36, new BytesRef("yang"), 1);

    // test default
    DefaultPassageFormatter formatter = new DefaultPassageFormatter();
    assertEquals(
        "<b>Yin yang loooooooooong</b>, <b>yin</b> gap <b>yang</b> yong",
        formatter.format(passages, content));
  }

  public void testReversedStartOffsetOrder() {
    String content =
        "When indexing data in Solr, each document is composed of various fields. "
            + "A document essentially represents a single record, and each document typically contains a unique ID field.";

    Passage[] passages = new Passage[2];
    passages[0] = new Passage();
    passages[0].setStartOffset(73);
    passages[0].setEndOffset(179);
    passages[0].setScore(1.8846991f);
    passages[0].addMatch(75, 83, new BytesRef("document"), 1);
    passages[0].addMatch(133, 141, new BytesRef("document"), 1);

    passages[1] = new Passage();
    passages[1].setStartOffset(0);
    passages[1].setEndOffset(73);
    passages[1].setScore(1.5923802f);
    passages[1].addMatch(33, 41, new BytesRef("document"), 1);

    DefaultPassageFormatter formatter = new DefaultPassageFormatter("<b>", "</b>", "\n", false);
    assertEquals(
        "A <b>document</b> essentially represents a single record, and each <b>document</b> typically contains a unique ID field.\n"
            + "When indexing data in Solr, each <b>document</b> is composed of various fields. ",
        formatter.format(passages, content));
  }
}
