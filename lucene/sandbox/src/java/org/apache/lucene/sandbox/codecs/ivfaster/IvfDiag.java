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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.util.Locale;
import org.apache.lucene.util.SuppressForbidden;

/**
 * The codec's diagnostic output, in one place.
 *
 * <p>Several stages can report what they did: which kernel loaded, how wide a build split resolved,
 * how long each build stage took, whether the graph descent ran or fell back to the exact scan.
 * Those reports make this package's performance claims checkable on the machine they are claimed
 * on.
 *
 * <p>EVERY caller is behind a system-property gate and nothing here runs by default, since a codec
 * must not write to the console in normal operation. Collected into one class so the {@link
 * SuppressForbidden} that console output requires appears once, with one justification.
 *
 * @lucene.experimental
 */
final class IvfDiag {

  private IvfDiag() {}

  /** Formatted diagnostic to stderr, in {@link Locale#ROOT} so traces are machine-comparable. */
  @SuppressForbidden(reason = "diagnostic trace, gated behind a system property and off by default")
  static void err(String format, Object... args) {
    System.err.printf(Locale.ROOT, format, args);
  }

  /** One-line diagnostic to stderr. */
  @SuppressForbidden(reason = "diagnostic trace, gated behind a system property and off by default")
  static void errln(String message) {
    System.err.println(message);
  }

  /**
   * One-line report to stdout, for the engagement counters a caller explicitly asked for. stdout,
   * since a requested result is not a warning.
   */
  @SuppressForbidden(
      reason = "engagement report, gated behind a system property and off by default")
  static void outln(String message) {
    System.out.println(message);
  }

  /**
   * Reports a swallowed throwable, for a fallback that is correct and silent. Prints the class and
   * message, since the fact of interest is which fallback engaged.
   */
  @SuppressForbidden(reason = "diagnostic trace, gated behind a system property and off by default")
  static void errThrowable(String context, Throwable t) {
    System.err.println(context + ": " + t);
  }
}
