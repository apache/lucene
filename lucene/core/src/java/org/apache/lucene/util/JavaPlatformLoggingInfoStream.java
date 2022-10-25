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

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * InfoStream implementation that logs every message using Java Platform Logging (<a
 * href="https://openjdk.org/jeps/264">JEP 264</a>) with the supplied log level.
 *
 * @lucene.internal
 */
public final class JavaPlatformLoggingInfoStream extends InfoStream {
  private final Map<String, Logger> cache = new ConcurrentHashMap<>();

  private final Function<String, String> componentToLoggerName;
  private final Level level;

  /**
   * Gets an implementation using the given log level with a default prefix {@code
   * "org.apache.lucene."}.
   *
   * @param level Requested log level to be used while logging
   */
  public JavaPlatformLoggingInfoStream(Level level) {
    this("org.apache.lucene.", level);
  }

  /**
   * Gets an implementation that logs using the given log level and adds a prefix to the component
   * name.
   *
   * @param namePrefix Prefix to be applied to all component names. It must be empty or include a
   *     final dot
   * @param level Requested log level to be used while logging
   */
  public JavaPlatformLoggingInfoStream(String namePrefix, Level level) {
    this(Objects.requireNonNull(namePrefix, "namePrefix")::concat, level);
  }

  /**
   * Gets an implementation that logs using the given log level with a logger name derived from the
   * component name.
   *
   * @param componentToLoggerName A function to convert a component name to a valid JUL logger name
   * @param level Requested log level to be used while logging
   */
  public JavaPlatformLoggingInfoStream(
      Function<String, String> componentToLoggerName, Level level) {
    this.componentToLoggerName =
        Objects.requireNonNull(componentToLoggerName, "componentToLoggerName");
    this.level = Objects.requireNonNull(level, "level");
  }

  @Override
  public void message(String component, String message) {
    getLogger(component).log(level, message);
  }

  @Override
  public boolean isEnabled(String component) {
    return getLogger(component).isLoggable(level);
  }

  @Override
  public void close() {
    cache.clear();
  }

  private Logger getLogger(String component) {
    return cache.computeIfAbsent(
        Objects.requireNonNull(component, "component"),
        c -> System.getLogger(componentToLoggerName.apply(c)));
  }
}
