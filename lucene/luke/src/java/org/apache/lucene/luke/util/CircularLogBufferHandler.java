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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.stream.Stream;

/** A {@link Handler} with a bounded buffer of recent log messages. */
public class CircularLogBufferHandler extends Handler {
  // Keep a bounded buffer of messages logged so far. We'll keep them
  // preformatted, no need to complicate things.
  private final ArrayDeque<String> buffer = new ArrayDeque<>();

  private final List<Consumer<CircularLogBufferHandler>> listeners = new CopyOnWriteArrayList<>();

  @Override
  public void publish(LogRecord record) {
    synchronized (buffer) {
      var formatted = format(record);
      buffer.addLast(formatted);
    }

    listeners.forEach(c -> c.accept(this));
  }

  private String format(LogRecord record) {
    return String.format(
        Locale.ROOT,
        "%s [%s]: %s",
        record.getLoggerName(),
        record.getLevel(),
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

  @Override
  public void flush() {
    // Ignore.
  }

  @Override
  public void close() throws SecurityException {
    // Ignore.
  }

  public Stream<String> getLogEntries() {
    synchronized (buffer) {
      return new ArrayList<>(buffer).stream();
    }
  }

  public void addUpdateListener(Consumer<CircularLogBufferHandler> listener) {
    listeners.add(listener);
  }
}
