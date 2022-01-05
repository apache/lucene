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

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Encapsulates access to the security manager, which is deprecated as of Java 17.
 *
 * @lucene.internal
 */
@SuppressWarnings("removal")
@SuppressForbidden(reason = "security manager")
public final class LegacySecurityManager {

  /** Delegates to {@link AccessController#doPrivileged(PrivilegedAction)}. */
  public static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  /**
   * Returns the {@link ThreadGroup} configured by the {@link SecurityManager} or the thread group
   * of the current thread if no security manager is configured.
   */
  public static ThreadGroup getThreadGroup() {
    final SecurityManager s = System.getSecurityManager();
    if (s != null) {
      return s.getThreadGroup();
    } else {
      return Thread.currentThread().getThreadGroup();
    }
  }
}
