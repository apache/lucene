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

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/** A {@link Handler} with a bounded buffer of recent log messages. */
public class CircularLogBufferHandler extends Handler {
  /** Provides an immutable clone of the required data from a {@link LogRecord} logged elsewhere. */
  public static final class ImmutableLogRecord {
    private final String loggerName;
    private final Level level;
    private final String message;
    private final Throwable thrown;
    private final Instant instant;

    public ImmutableLogRecord(LogRecord record) {
      this.loggerName = record.getLoggerName();
      this.level = record.getLevel();
      this.message = record.getMessage();
      this.thrown = record.getThrown();
      this.instant = record.getInstant();
    }

    public String getLoggerName() {
      return loggerName;
    }

    public Level getLevel() {
      return level;
    }

    public String getMessage() {
      return message;
    }

    public Throwable getThrown() {
      return thrown;
    }

    public Instant getInstant() {
      return instant;
    }
  }

  /** Listeners receiving log state updates. */
  public interface LogUpdateListener extends Consumer<Collection<ImmutableLogRecord>> {}

  /** A bounded buffer of immutable log records that have been recently logged by the framework. */
  private final ArrayDeque<ImmutableLogRecord> buffer = new ArrayDeque<>();

  /**
   * Listeners interested in receiving log state updates. At the moment listeners receive an
   * iterable with all log records in the circular buffer. We could make it incremental for each
   * consumer but for Luke it really doesn't matter since the rate of updates is very small.
   */
  private final List<LogUpdateListener> listeners = new CopyOnWriteArrayList<>();

  @Override
  public void publish(LogRecord record) {
    synchronized (buffer) {
      buffer.addLast(new ImmutableLogRecord(record));
      listeners.forEach(c -> c.accept(buffer));
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

  public void addUpdateListener(LogUpdateListener listener) {
    listeners.add(listener);
  }

  public void removeUpdateListener(LogUpdateListener listener) {
    listeners.remove(listener);
  }

  /** @return Return a clone of the buffered records so far. */
  public List<ImmutableLogRecord> getLogRecords() {
    synchronized (buffer) {
      return List.copyOf(buffer);
    }
  }
}
