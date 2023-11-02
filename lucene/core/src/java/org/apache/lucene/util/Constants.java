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

/** Some useful constants. */
public final class Constants {
  private Constants() {} // can't construct

  /** JVM vendor info. */
  public static final String JVM_VENDOR = System.getProperty("java.vm.vendor");

  /** JVM vendor name. */
  public static final String JVM_NAME = System.getProperty("java.vm.name");

  /** The value of <code>System.getProperty("os.name")</code>. * */
  public static final String OS_NAME = System.getProperty("os.name");

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
  public static final String OS_ARCH = System.getProperty("os.arch");

  /** The value of <code>System.getProperty("os.version")</code>. */
  public static final String OS_VERSION = System.getProperty("os.version");

  /** The value of <code>System.getProperty("java.vendor")</code>. */
  public static final String JAVA_VENDOR = System.getProperty("java.vendor");

  /** True iff running on a 64bit JVM */
  public static final boolean JRE_IS_64BIT;

  static {
    boolean is64Bit = false;
    String datamodel = null;
    try {
      datamodel = System.getProperty("sun.arch.data.model");
      if (datamodel != null) {
        is64Bit = datamodel.contains("64");
      }
    } catch (
        @SuppressWarnings("unused")
        SecurityException ex) {
    }
    if (datamodel == null) {
      if (OS_ARCH != null && OS_ARCH.contains("64")) {
        is64Bit = true;
      } else {
        is64Bit = false;
      }
    }
    JRE_IS_64BIT = is64Bit;
  }

  private static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
  private static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

  // best effort to see if FMA is fast (this is architecture-independent option)
  private static boolean hasFastFMA() {
    try {
      final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
      // we use reflection for this, because the management factory is not part
      // of Java 8's compact profile:
      final Object hotSpotBean =
          Class.forName(MANAGEMENT_FACTORY_CLASS)
              .getMethod("getPlatformMXBean", Class.class)
              .invoke(null, beanClazz);
      if (hotSpotBean != null) {
        final var getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
        final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "UseFMA");
        boolean hasFMA =
            Boolean.parseBoolean(
                vmOption.getClass().getMethod("getValue").invoke(vmOption).toString());
        if (hasFMA) {
          if (OS_ARCH.equals("amd64")) {
            // we've got FMA, but detect if its AMD and avoid it in that case
            final Object vmOption2 = getVMOptionMethod.invoke(hotSpotBean, "UseXmmI2F");
            hasFMA =
                !Boolean.parseBoolean(
                    vmOption2.getClass().getMethod("getValue").invoke(vmOption2).toString());
          } else if (OS_ARCH.equals("aarch64")) {
            // we've got FMA, but its slower unless its a newer SVE-based chip
            final Object vmOption2 = getVMOptionMethod.invoke(hotSpotBean, "UseSVE");
            hasFMA =
                Integer.parseInt(
                        vmOption2.getClass().getMethod("getValue").invoke(vmOption2).toString())
                    > 0;
          }
        }
        return hasFMA;
      }
      return false;
    } catch (@SuppressWarnings("unused") ReflectiveOperationException | RuntimeException e) {
      return false;
    }
  }

  /** true if we know FMA is supported and fast, to deliver less error */
  public static final boolean HAS_FAST_FMA = hasFastFMA();
}
