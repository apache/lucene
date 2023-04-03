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
package org.apache.lucene.search.vectorhighlight;

public class TestBaseFragmentsBuilder extends AbstractTestCase {

  public void testDiscreteMultiValueHighlightingWhenSecondFieldIsTheBestMatch() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery(tq("d"), true, true);
    FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
    FieldPhraseList fpl = new FieldPhraseList(stack, fq);
    SingleFragListBuilder sflb = new SingleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList(fpl, Integer.MAX_VALUE);
    ScoreOrderFragmentsBuilder sfb = new ScoreOrderFragmentsBuilder();
    sfb.setDiscreteMultiValueHighlighting(true);
    assertEquals("<b>d</b> e", sfb.createFragment(reader, 0, F, ffl));

    make1dmfIndex("First text to highlight", "Second text to highlight in a text field");
    fq = new FieldQuery(tq("text"), true, true);
    stack = new FieldTermStack(reader, 0, F, fq);
    fpl = new FieldPhraseList(stack, fq);
    sflb = new SingleFragListBuilder();
    ffl = sflb.createFieldFragList(fpl, Integer.MAX_VALUE);
    String[] result = sfb.createFragments(reader, 0, F, ffl, 3);
    assertEquals(2, result.length);
    assertEquals("Second <b>text</b> to highlight in a <b>text</b> field", result[0]);
    assertEquals("First <b>text</b> to highlight", result[1]);

    fq = new FieldQuery(tq("highlight"), true, true);
    stack = new FieldTermStack(reader, 0, F, fq);
    fpl = new FieldPhraseList(stack, fq);
    sflb = new SingleFragListBuilder();
    ffl = sflb.createFieldFragList(fpl, Integer.MAX_VALUE);
    result = sfb.createFragments(reader, 0, F, ffl, 3);
    assertEquals(2, result.length);
    assertEquals("First text to <b>highlight</b>", result[0]);
    assertEquals("Second text to <b>highlight</b> in a text field", result[1]);
  }

}
