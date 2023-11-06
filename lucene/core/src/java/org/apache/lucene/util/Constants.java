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

  /**
   * Get the full version string of the current runtime.
   *
   * @deprecated To detect Java versions use {@link Runtime#version()}
   */
  @Deprecated public static final String JVM_VERSION = Runtime.version().toString();

  /**
   * Gets the specification version of the current runtime. This is the feature version converted to
   * String.
   *
   * @see java.lang.Runtime.Version#feature()
   * @deprecated To detect Java versions use {@link Runtime#version()}
   */
  @Deprecated
  public static final String JVM_SPEC_VERSION = Integer.toString(Runtime.version().feature());

  /**
   * The value of <code>System.getProperty("java.version")</code>.
   *
   * @deprecated To detect Java versions use {@link Runtime#version()}
   */
  @Deprecated public static final String JAVA_VERSION = System.getProperty("java.version");

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

  /** True iff the Java runtime is a client runtime and C2 compiler is not enabled. */
  public static final boolean IS_CLIENT_VM =
      getSysProp("java.vm.info", "").contains("emulated-client");

  /** True iff the Java VM is based on Hotspot and has the Hotspot MX bean readable by Lucene. */
  public static final boolean IS_HOTSPOT_VM = HotspotVMOptions.IS_HOTSPOT_VM;

  /** True if jvmci is enabled (e.g. graalvm) */
  public static final boolean IS_JVMCI_VM =
      HotspotVMOptions.get("UseJVMCICompiler").map(Boolean::valueOf).orElse(false);

  /** True iff running on a 64bit JVM */
  public static final boolean JRE_IS_64BIT = is64Bit();

  private static boolean is64Bit() {
    final String datamodel = getSysProp("sun.arch.data.model");
    if (datamodel != null) {
      return datamodel.contains("64");
    } else {
      return (OS_ARCH != null && OS_ARCH.contains("64"));
    }
  }

  /** true if FMA likely means a cpu instruction and not BigDecimal logic. */
  private static final boolean HAS_FMA =
      (IS_CLIENT_VM == false) && HotspotVMOptions.get("UseFMA").map(Boolean::valueOf).orElse(false);

  /** maximum supported vectorsize. */
  private static final int MAX_VECTOR_SIZE =
      HotspotVMOptions.get("MaxVectorSize").map(Integer::valueOf).orElse(0);

  /** true for an AMD cpu with SSE4a instructions. */
  private static final boolean HAS_SSE4A =
      HotspotVMOptions.get("UseXmmI2F").map(Boolean::valueOf).orElse(false);

  /** true iff we know VFMA has faster throughput than separate vmul/vadd. */
  public static final boolean HAS_FAST_VECTOR_FMA = hasFastVectorFMA();

  /** true iff we know FMA has faster throughput than separate mul/add. */
  public static final boolean HAS_FAST_SCALAR_FMA = hasFastScalarFMA();

  private static boolean hasFastVectorFMA() {
    if (HAS_FMA) {
      String value = getSysProp("lucene.useVectorFMA", "auto");
      if ("auto".equals(value)) {
        // newer Neoverse cores have their act together
        // the problem is just apple silicon (this is a practical heuristic)
        if (OS_ARCH.equals("aarch64") && MAC_OS_X == false) {
          return true;
        }
        // zen cores or newer, its a wash, turn it on as it doesn't hurt
        // starts to yield gains for vectors only at zen4+
        if (HAS_SSE4A && MAX_VECTOR_SIZE >= 32) {
          return true;
        }
        // intel has their act together
        if (OS_ARCH.equals("amd64") && HAS_SSE4A == false) {
          return true;
        }
      } else {
        return Boolean.parseBoolean(value);
      }
    }
    // everyone else is slow, until proven otherwise by benchmarks
    return false;
  }

  private static boolean hasFastScalarFMA() {
    if (HAS_FMA) {
      String value = getSysProp("lucene.useScalarFMA", "auto");
      if ("auto".equals(value)) {
        // newer Neoverse cores have their act together
        // the problem is just apple silicon (this is a practical heuristic)
        if (OS_ARCH.equals("aarch64") && MAC_OS_X == false) {
          return true;
        }
        // latency becomes 4 for the Zen3 (0x19h), but still a wash
        // until the Zen4 anyway, and big drop on previous zens:
        if (HAS_SSE4A && MAX_VECTOR_SIZE >= 64) {
          return true;
        }
        // intel has their act together
        if (OS_ARCH.equals("amd64") && HAS_SSE4A == false) {
          return true;
        }
      } else {
        return Boolean.parseBoolean(value);
      }
    }
    // everyone else is slow, until proven otherwise by benchmarks
    return false;
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

  /**
   * Always true.
   *
   * @deprecated This constant is useless and always {@code true}. To detect Java versions use
   *     {@link Runtime#version()}
   */
  @Deprecated public static final boolean JRE_IS_MINIMUM_JAVA8 = true;

  /**
   * Always true.
   *
   * @deprecated This constant is useless and always {@code true}. To detect Java versions use
   *     {@link Runtime#version()}
   */
  @Deprecated public static final boolean JRE_IS_MINIMUM_JAVA9 = true;

  /**
   * Always true.
   *
   * @deprecated This constant is useless and always {@code true}. To detect Java versions use
   *     {@link Runtime#version()}
   */
  @Deprecated public static final boolean JRE_IS_MINIMUM_JAVA11 = true;
}
