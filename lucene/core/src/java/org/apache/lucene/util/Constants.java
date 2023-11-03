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
import java.util.logging.Logger;

/** Some useful constants. */
public final class Constants {
  private Constants() {} // can't construct

  private static final String UNKNOWN = "Unknown";

  /** JVM vendor info. */
  public static final String JVM_VENDOR = getSysProp("java.vm.vendor", UNKNOWN);

  /** JVM vendor name. */
  public static final String JVM_NAME = getSysProp("java.vm.name", UNKNOWN);

  /** The value of <code>System.getProperty("os.name")</code>. * */
  public static final String OS_NAME = getSysProp("os.name", UNKNOWN);

  /** True iff running on Linux. */
  public static final boolean LINUX = OS_NAME.startsWith("Linux");

  /** True iff running on Windows. */
  public static final boolean WINDOWS = OS_NAME.startsWith("Windows");

  /** True iff running on SunOS. */
  public static final boolean SUN_OS = OS_NAME.startsWith("SunOS");

  /** True iff running on Mac OS X */
  public static final boolean MAC_OS_X = OS_NAME.startsWith("Mac OS X");

  /** True iff running on FreeBSD */
  public static final boolean FREE_BSD = OS_NAME.startsWith("FreeBSD");

  /** The value of <code>System.getProperty("os.arch")</code>. */
  public static final String OS_ARCH = getSysProp("os.arch", UNKNOWN);

  /** The value of <code>System.getProperty("os.version")</code>. */
  public static final String OS_VERSION = getSysProp("os.version", UNKNOWN);

  /** The value of <code>System.getProperty("java.vendor")</code>. */
  public static final String JAVA_VENDOR = getSysProp("java.vendor", UNKNOWN);

  /** True iff the Java runtime is a client runtime and C2 compiler is not enabled */
  public static final boolean IS_CLIENT_VM =
      getSysProp("java.vm.info", "").contains("emulated-client");

  /** True iff running on a 64bit JVM */
  public static final boolean JRE_IS_64BIT = is64Bit();

  /** true iff we know fast FMA is supported, to deliver less error */
  public static final boolean HAS_FAST_FMA = (IS_CLIENT_VM == false) && hasFastFMA();

  private static boolean is64Bit() {
    final String datamodel = getSysProp("sun.arch.data.model");
    if (datamodel != null) {
      return datamodel.contains("64");
    } else {
      return (OS_ARCH != null && OS_ARCH.contains("64"));
    }
  }

  // best effort to see if FMA is fast (this is architecture-independent option)
  private static boolean hasFastFMA() {
    boolean hasFMA = HotspotVMOptions.get("UseFMA").map(Boolean::valueOf).orElse(false);
    if (hasFMA) {
      if (OS_ARCH.equals("amd64")) {
        // we've got FMA, but detect if its AMD and avoid it in that case
        hasFMA = HotspotVMOptions.get("UseXmmI2F").map(Boolean::valueOf).orElse(false);
      } else if (OS_ARCH.equals("aarch64")) {
        // we've got FMA, but its slower unless its a newer SVE-based chip
        hasFMA = HotspotVMOptions.get("UseSVE").map(Integer::valueOf).orElse(0) > 0;
      }
    }
    return hasFMA;
  }

  private static String getSysProp(String property) {
    try {
      return doPrivileged(() -> System.getProperty(property));
    } catch (
        @SuppressWarnings("unused")
        SecurityException se) {
      logSecurityWarning(property);
      return null;
    }
  }

  private static String getSysProp(String property, String def) {
    try {
      return doPrivileged(() -> System.getProperty(property, def));
    } catch (
        @SuppressWarnings("unused")
        SecurityException se) {
      logSecurityWarning(property);
      return def;
    }
  }

  private static void logSecurityWarning(String property) {
    var log = Logger.getLogger(Constants.class.getName());
    log.warning("SecurityManager prevented access to system property: " + property);
  }

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }
}
