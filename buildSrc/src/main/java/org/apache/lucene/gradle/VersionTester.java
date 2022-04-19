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
package org.apache.lucene.gradle;

/**
 * Standalone class that can be used to test if java is too new.
 * <p>
 * Otherwise, you get a very unclear error message from gradle
 * <p>
 * Has no dependencies outside of standard java libraries
 */
public class VersionTester {
  public static void main(String[] args) {
    int major = Runtime.getRuntime().version().feature();
    if (major > 17) {
      System.err.println("java version must not be newer than 17 (unsupported by gradle), your version: " + major);
      System.exit(1);
    }
  }
}
