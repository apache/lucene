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

package org.apache.lucene.tests.util;

import static org.apache.lucene.tests.util.LuceneTestCaseParent.DEFAULT_LINE_DOCS_FILE;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.JENKINS_LARGE_LINE_DOCS_FILE;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.RANDOM_MULTIPLIER;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.SYSPROP_AWAITSFIX;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.SYSPROP_MONSTER;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.SYSPROP_NIGHTLY;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.SYSPROP_WEEKLY;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_ASSERTS_ENABLED;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_AWAITSFIX;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_CODEC;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_DIRECTORY;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_DOCVALUESFORMAT;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_LINE_DOCS_FILE;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_MONSTER;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_NIGHTLY;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_POSTINGSFORMAT;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.TEST_WEEKLY;
import static org.apache.lucene.tests.util.LuceneTestCaseParent.defaultRandomMultiplier;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Constants;

/** Test environment information. */
record TestEnvInfo(String codec, String similarity, String locale, String timeZone) {
  static final String TEST_ENV_LEAD = "NOTE: test environment is:";
  static final String TEST_REPRO_LEAD = "NOTE: reproduce with:";

  TestEnvInfo(Codec codec, Similarity similarity, Locale locale, TimeZone timeZone) {
    this(
        Objects.toString(codec),
        Objects.toString(similarity),
        Optional.ofNullable(locale).map(Locale::toLanguageTag).orElse(null),
        Optional.ofNullable(timeZone).map(TimeZone::getID).orElse(null));
  }

  /** print some useful debugging information about the environment */
  String getDebuggingInformation() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println(
        (TEST_ENV_LEAD + " codec=" + codec)
            + (", sim=" + similarity)
            + (", locale=" + locale)
            + (", timezone=" + timeZone));
    pw.println(
        "NOTE: "
            + (System.getProperty("os.name") + " ")
            + (System.getProperty("os.version") + " ")
            + (System.getProperty("os.arch") + "/" + System.getProperty("java.vendor"))
            + (" " + System.getProperty("java.version"))
            + (" "
                + (Constants.JRE_IS_64BIT ? "(64-bit)" : "(32-bit)")
                + "/"
                + "cpus="
                + Runtime.getRuntime().availableProcessors()
                + ",")
            + ("threads=" + Thread.activeCount() + ",")
            + ("free=" + Runtime.getRuntime().freeMemory() + ",")
            + ("total=" + Runtime.getRuntime().totalMemory()));
    pw.flush();
    return sw.toString();
  }

  String getAdditionalFailureInfo(String seed, Consumer<StringBuilder> extraArguments) {
    if (TEST_LINE_DOCS_FILE.endsWith(JENKINS_LARGE_LINE_DOCS_FILE)) {
      System.err.println(
          "NOTE: large line-docs file was used in this run. You have to download "
              + "it manually ('gradlew getEnWikiRandomLines') and use -P"
              + TEST_LINE_DOCS_FILE
              + "=... property to point to it.");
    }

    final StringBuilder b = new StringBuilder();
    b.append(TEST_REPRO_LEAD + " gradlew test ");

    extraArguments.accept(b);

    // Pass the master seed.
    addVmOpt(b, "tests.seed", seed);
    addVmOpt(b, "tests.jvmargs", System.getProperty("tests.jvmargs"));

    // Test groups and multipliers.
    if (RANDOM_MULTIPLIER != defaultRandomMultiplier())
      addVmOpt(b, "tests.multiplier", RANDOM_MULTIPLIER);
    if (TEST_NIGHTLY) addVmOpt(b, SYSPROP_NIGHTLY, TEST_NIGHTLY);
    if (TEST_WEEKLY) addVmOpt(b, SYSPROP_WEEKLY, TEST_WEEKLY);
    if (TEST_MONSTER) addVmOpt(b, SYSPROP_MONSTER, TEST_MONSTER);
    if (TEST_AWAITSFIX) addVmOpt(b, SYSPROP_AWAITSFIX, TEST_AWAITSFIX);

    // Codec, postings, directories.
    if (!TEST_CODEC.equals("random")) addVmOpt(b, "tests.codec", TEST_CODEC);
    if (!TEST_POSTINGSFORMAT.equals("random"))
      addVmOpt(b, "tests.postingsformat", TEST_POSTINGSFORMAT);
    if (!TEST_DOCVALUESFORMAT.equals("random"))
      addVmOpt(b, "tests.docvaluesformat", TEST_DOCVALUESFORMAT);
    if (!TEST_DIRECTORY.equals("random")) addVmOpt(b, "tests.directory", TEST_DIRECTORY);

    // Environment.
    if (!TEST_LINE_DOCS_FILE.equals(DEFAULT_LINE_DOCS_FILE))
      addVmOpt(b, "tests.linedocsfile", TEST_LINE_DOCS_FILE);
    if (locale() != null) {
      addVmOpt(b, "tests.locale", locale());
    }

    if (timeZone() != null) {
      addVmOpt(b, "tests.timezone", timeZone());
    }

    if (TEST_ASSERTS_ENABLED) {
      addVmOpt(b, "tests.asserts", "true");
    } else {
      addVmOpt(b, "tests.asserts", "false");
    }

    addVmOpt(b, "tests.file.encoding", System.getProperty("file.encoding"));

    return b.toString();
  }

  /**
   * Append a VM option (-Dkey=value) to a {@link StringBuilder}. Add quotes if spaces or other
   * funky characters are detected.
   */
  static void addVmOpt(StringBuilder b, String key, Object value) {
    if (value == null) return;

    b.append(" -D").append(key).append("=");
    String v = value.toString();
    // Add simplistic quoting. This varies a lot from system to system and between
    // shells... ANT should have some code for doing it properly.
    if (Pattern.compile("[\\s=']").matcher(v).find()) {
      v = '"' + v + '"';
    }
    b.append(v);
  }
}
