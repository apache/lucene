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

import java.util.Locale;
import java.util.Optional;
import org.apache.lucene.store.ReadAdvice;

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

  /** true for cpu with AVX support at least AVX2. */
  private static final boolean HAS_AVX2 =
      HotspotVMOptions.get("UseAVX").map(Integer::valueOf).orElse(0) >= 2;

  /** true for arm cpu with SVE support. */
  private static final boolean HAS_SVE =
      HotspotVMOptions.get("UseSVE").map(Integer::valueOf).orElse(0) >= 1;

  /** true iff we know VFMA has faster throughput than separate vmul/vadd. */
  public static final boolean HAS_FAST_VECTOR_FMA = hasFastVectorFMA();

  /** true iff we know FMA has faster throughput than separate mul/add. */
  public static final boolean HAS_FAST_SCALAR_FMA = hasFastScalarFMA();

  /** true iff we know Compress and Cast has fast throughput. */
  public static final boolean HAS_FAST_COMPRESS_MASK_CAST = hasFastCompressMaskCast();

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

  private static boolean hasFastCompressMaskCast() {
    if (OS_ARCH.equals("aarch64") && HAS_SVE) {
      return true;
    }

    if (OS_ARCH.equals("amd64") && HAS_AVX2) {
      return true;
    }
    return false;
  }

  /**
   * The default {@link ReadAdvice} used for opening index files. It will be {@link
   * ReadAdvice#NORMAL} by default, unless set by system property {@code
   * org.apache.lucene.store.defaultReadAdvice}.
   */
  public static final ReadAdvice DEFAULT_READADVICE =
      Optional.ofNullable(getSysProp("org.apache.lucene.store.defaultReadAdvice"))
          .map(a -> ReadAdvice.valueOf(a.toUpperCase(Locale.ROOT)))
          .orElse(ReadAdvice.NORMAL);

  private static String getSysProp(String property) {
    return System.getProperty(property);
  }

  private static String getSysProp(String property, String def) {
    return System.getProperty(property, def);
  }
}
