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
package org.apache.lucene.util;

import java.util.Enumeration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestJavaLoggingInfoStream extends LuceneTestCase {

  public void test() throws Exception {
    assumeTrue(
        "invalid logging configuration for testing", Logger.getGlobal().isLoggable(Level.WARNING));
    final var component = "TESTCOMPONENT" + random().nextInt();
    final var loggerName = "org.apache.lucene." + component;
    try (var stream = new JavaLoggingInfoStream(Level.WARNING)) {
      assertTrue(stream.isEnabled(component));
      stream.message(component, "Test message to be logged.");
    }
    assertTrue(
        "logger with correct name not found",
        streamOfEnumeration(LogManager.getLogManager().getLoggerNames())
            .anyMatch(loggerName::equals));
  }

  private static <T> Stream<T> streamOfEnumeration(Enumeration<T> e) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(e.asIterator(), Spliterator.ORDERED), false);
  }
}
