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

import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/** Logger factory. This configures log interceptors for the GUI. */
public class LoggerFactory {

  public static void initGuiLogging() {
    var rootLogger = LogManager.getLogManager().getLogger("");

    rootLogger.addHandler(
        new Handler() {
          @Override
          public void publish(LogRecord record) {}

          @Override
          public void flush() {
            // Ignore.
          }

          @Override
          public void close() throws SecurityException {
            // Ignore.
          }
        });
  }

  public static Logger getLogger(Class<?> clazz) {
    return LogManager.getLogManager().getLogger(clazz.getName());
  }
}
