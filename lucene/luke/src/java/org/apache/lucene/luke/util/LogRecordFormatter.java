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

package org.apache.lucene.luke.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.Function;

/** Formats {@link CircularLogBufferHandler.ImmutableLogRecord} to string. */
public class LogRecordFormatter
    implements Function<CircularLogBufferHandler.ImmutableLogRecord, String> {
  @Override
  public String apply(CircularLogBufferHandler.ImmutableLogRecord record) {
    return String.format(
        Locale.ROOT,
        "%s [%s] %s: %s",
        DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)
            .format(record.getInstant().atZone(ZoneId.systemDefault())),
        record.getLevel(),
        record.getLoggerName(),
        record.getMessage()
            + (record.getThrown() == null ? "" : "\n" + toString(record.getThrown())));
  }

  private String toString(Throwable t) {
    try (StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw)) {
      t.printStackTrace(pw);
      pw.flush();
      return sw.toString();
    } catch (IOException e) {
      return "Could not dump stack trace: " + e.getMessage();
    }
  }
}
