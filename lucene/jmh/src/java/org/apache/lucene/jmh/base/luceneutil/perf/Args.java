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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The type Args. */
public class Args {

  private final String[] args;
  private final Map<String, Boolean> used = new HashMap<String, Boolean>();

  /**
   * Instantiates a new Args.
   *
   * @param args the args
   */
  public Args(String[] args) {
    this.args = args;
  }

  /**
   * Gets string.
   *
   * @param argName the arg name
   * @return the string
   */
  public String getString(String argName) {
    for (int upto = 0; upto < args.length; upto++) {
      if (args[upto].equals(argName)) {
        if (upto == args.length - 1) {
          throw new RuntimeException("missing value for argument " + argName);
        }
        used.put(argName, true);
        return args[1 + upto];
      }
    }

    throw new RuntimeException("missing required argument: " + argName);
  }

  /**
   * Gets strings.
   *
   * @param argName the arg name
   * @return the strings
   */
  public List<String> getStrings(String argName) {
    List<String> values = new ArrayList<String>();
    for (int upto = 0; upto < args.length; upto++) {
      if (args[upto].equals(argName)) {
        if (upto == args.length - 1) {
          throw new RuntimeException("missing value for argument " + argName);
        }
        used.put(argName, true);
        values.add(args[1 + upto]);
      }
    }
    if (values.size() == 0) {
      throw new RuntimeException("missing required argument: " + argName);
    }
    return values;
  }

  /**
   * Gets string.
   *
   * @param argName the arg name
   * @param defaultValue the default value
   * @return the string
   */
  public String getString(String argName, String defaultValue) {
    for (int upto = 0; upto < args.length; upto++) {
      if (args[upto].equals(argName)) {
        if (upto == args.length - 1) {
          throw new RuntimeException("missing value for argument " + argName);
        }
        used.put(argName, true);
        return args[1 + upto];
      }
    }

    return defaultValue;
  }

  /**
   * Gets int.
   *
   * @param argName the arg name
   * @return the int
   */
  public int getInt(String argName) {
    return Integer.parseInt(getString(argName));
  }

  /**
   * Gets double.
   *
   * @param argName the arg name
   * @return the double
   */
  public double getDouble(String argName) {
    return Double.parseDouble(getString(argName));
  }

  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  //  /**
  //   * Gets float.
  //   *
  //   * @param argName the arg name
  //   * @return the float
  //   */
  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  ////  public float getFloat(String argName) {
  ////    return Float.parseFloat(getString(argName));
  ////  }
  //// --Commented out by Inspection STOP (10/7/21, 12:37 AM)
  //

  /**
   * Gets long.
   *
   * @param argName the arg name
   * @return the long
   */
  public long getLong(String argName) {
    return Long.parseLong(getString(argName));
  }

  /**
   * Gets flag.
   *
   * @param argName the arg name
   * @return the flag
   */
  public boolean getFlag(String argName) {
    for (int upto = 0; upto < args.length; upto++) {
      if (args[upto].equals(argName)) {
        used.put(argName, false);
        return true;
      }
    }

    return false;
  }

  /**
   * Has arg boolean.
   *
   * @param argName the arg name
   * @return the boolean
   */
  public boolean hasArg(String argName) {
    for (int upto = 0; upto < args.length; upto++) {
      if (args[upto].equals(argName)) {
        return true;
      }
    }

    return false;
  }

  /** Check. */
  public void check() {
    for (int upto = 0; upto < args.length; upto++) {
      Boolean v = used.get(args[upto]);
      if (v == null) {
        throw new RuntimeException("argument " + args[upto] + " isn't recognized");
      } else if (v) {
        upto++;
      }
    }
  }
}
