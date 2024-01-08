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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * Standalone class that generates a populated gradle.properties from a template.
 *
 * <p>Has no dependencies outside of standard java libraries
 */
public class GradlePropertiesGenerator {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: java GradlePropertiesGenerator.java <source> <destination>");
      System.exit(2);
    }

    try {
      new GradlePropertiesGenerator().run(Paths.get(args[0]), Paths.get(args[1]));
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(3);
    }
  }

  public void run(Path source, Path destination) throws IOException {
    if (!Files.exists(source)) {
      throw new IOException("template file not found: " + source);
    }
    if (Files.exists(destination)) {
      System.out.println(destination + " already exists, skipping generation.");
      return;
    }

    // Approximate a common-sense default for running gradle/tests with parallel
    // workers: half the count of available cpus but not more than 12.
    var cpus = Runtime.getRuntime().availableProcessors();
    var maxWorkers = (int) Math.max(1d, Math.min(cpus * 0.5d, 12));
    var testsJvms = (int) Math.max(1d, Math.min(cpus * 0.5d, 12));

    var replacements = Map.of("@MAX_WORKERS@", maxWorkers, "@TEST_JVMS@", testsJvms);

    System.out.println("Generating gradle.properties");
    String fileContent = Files.readString(source, StandardCharsets.UTF_8);
    for (var entry : replacements.entrySet()) {
      fileContent = fileContent.replace(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Files.writeString(
            destination, fileContent, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
  }
}
