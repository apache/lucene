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
package org.apache.lucene.analysis.tests;

import java.net.URI;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.junit.Assert;

/** Discovers all classes from the module graph and loads them (without initialization) */
public class ModuleClassDiscovery {

  public static List<Class<?>> getClassesForPackage(String pckgname) throws Exception {
    final List<Class<?>> classes = new ArrayList<>();
    collectClassesForPackage(pckgname, classes);
    Assert.assertFalse(
        "No classes found in package '"
            + pckgname
            + "'; maybe your test classes are packaged as JAR file?",
        classes.isEmpty());
    return classes;
  }

  private static void collectClassesForPackage(String pckgname, List<Class<?>> classes)
      throws Exception {
    final ClassLoader cld = TestRandomChains.class.getClassLoader();
    final String path = pckgname.replace('.', '/');
    final Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final URI uri = resources.nextElement().toURI();
      if (!"file".equalsIgnoreCase(uri.getScheme())) continue;
      final Path directory = Paths.get(uri);
      if (Files.exists(directory)) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
          for (Path file : stream) {
            if (Files.isDirectory(file)) {
              // recurse
              String subPackage = pckgname + "." + file.getFileName().toString();
              collectClassesForPackage(subPackage, classes);
            }
            String fname = file.getFileName().toString();
            if (fname.endsWith(".class")) {
              String clazzName = fname.substring(0, fname.length() - 6);
              // exclude Test classes that happen to be in these packages.
              // class.ForName'ing some of them can cause trouble.
              if (!clazzName.endsWith("Test") && !clazzName.startsWith("Test")) {
                // Don't run static initializers, as we won't use most of them.
                // Java will do that automatically once accessed/instantiated.
                classes.add(Class.forName(pckgname + '.' + clazzName, false, cld));
              }
            }
          }
        }
      }
    }
  }

}
