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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;

public class TestScandinavianNormalizationFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testDefault() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("räksmörgås_ae_oe_aa_oo_ao_AE_OE_AA_OO_AO");
    stream = tokenFilterFactory("ScandinavianNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] {"ræksmørgås_æ_ø_å_ø_å_Æ_Ø_Å_Ø_Å"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("ScandinavianNormalization", "bogusArg", "bogusValue");
            });
    assertTrue(
        "Got " + expected.getMessage(), expected.getMessage().contains("Unknown parameters"));
  }
}
