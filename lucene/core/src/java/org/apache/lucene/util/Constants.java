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
