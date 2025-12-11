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
package org.apache.lucene.gradle.plugins.licenses;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskContainer;

/**
 * This plugin configures tasks that collect Lucene's runtime and test dependencies, then for each
 * such dependency compute its checksum and verify that it has a proper corresponding license and
 * notice file under {@code lucene/licenses} folder.
 */
public class CheckJarChecksumsAndLicensesPlugin extends LuceneGradlePlugin {
  private record LicenseType(String acronym, String name, boolean noticeOptional) {
    LicenseType(String acronym, String name) {
      this(acronym, name, false);
    }
  }

  /**
   * All known license types. If {@link LicenseType#noticeOptional()} is true then the notice file
   * must accompany the license.
   */
  private static final Map<String, LicenseType> LICENSE_TYPE =
      Stream.of(
              new LicenseType("ASL", "Apache Software License 2.0"),
              new LicenseType("BSD", "Berkeley Software Distribution"),
              // BSD-like just means someone has taken the BSD license and put in their
              // name, copyright, or it's a very similar license.
              new LicenseType("BSD_LIKE", "BSD-like license"),
              new LicenseType("CDDL", "Common Development and Distribution License", true),
              new LicenseType("CPL", "Common Public License"),
              new LicenseType("EPL", "Eclipse Public License Version 1.0", true),
              new LicenseType("MIT", "Massachusetts Institute of Tech. License", true),
              new LicenseType("MPL", "Mozilla Public License", true),
              new LicenseType("PD", "Public Domain", true),
              new LicenseType("PDDL", "Public Domain Dedication and License", true),
              new LicenseType("SUN", "Sun Open Source License", true),
              new LicenseType("COMPOUND", "Compound license (details in NOTICE file)."))
          .collect(Collectors.toMap(e -> e.acronym, e -> e));

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    TaskContainer tasks = project.getTasks();

    Directory licensesDir =
        project.project(":lucene").getLayout().getProjectDirectory().dir("licenses");

    // we can assume the java plugin has been applied already.
    var ignoredProjects = Set.of(":lucene:benchmark-jmh", ":build-tools:build-infra-shadow");
    for (var p : project.getAllprojects()) {
      if (p.getPlugins().hasPlugin(JavaPlugin.class) && !ignoredProjects.contains(p.getPath())) {
        configureJarInfoSourceProject(p);
      }
    }

    // create top-level tasks.
    var writeChecksums =
        tasks.register(
            "writeChecksums",
            CollectJarInfos.class,
            task -> {
              task.doFirst(_ -> writeChecksums(task, licensesDir));
            });

    var checkLicensesFolder =
        tasks.register(
            "checkLicensesFolder",
            CollectJarInfos.class,
            task -> {
              task.mustRunAfter(writeChecksums);
              task.doFirst(_ -> checkLicensesFolder(task, licensesDir.getAsFile()));
            });
    tasks.named("check").configure(task -> task.dependsOn(checkLicensesFolder));

    // aliases for compatibility with 10x.
    tasks.register("updateLicenses", task -> task.dependsOn(writeChecksums));
    tasks.register("licenses", task -> task.dependsOn(checkLicensesFolder));

    // connect tasks that aggregate jar infos with their sources.
    tasks
        .withType(CollectJarInfos.class)
        .configureEach(
            task -> {
              task.getJarInfos()
                  .from(
                      task.getProject().getAllprojects().stream()
                          .map(p -> p.getTasks().withType(ComputeJarInfos.class))
                          .toList());
            });
  }

  /** Sanity-check the licenses folder (checksums, license type, notice file), dangling files. */
  private static void checkLicensesFolder(CollectJarInfos task, File licensesDir) {
    List<String> errors = new ArrayList<>();
    List<File> referencedFiles = new ArrayList<>();

    final Pattern licenseNamePattern = Pattern.compile("LICENSE-(.+)\\.txt$");

    for (var jarInfo : task.getUniqueJarInfos()) {
      inspectLicenseAndNoticeFile(
          task, licensesDir, jarInfo, errors, referencedFiles, licenseNamePattern);
      verifyChecksum(licensesDir, jarInfo, errors, referencedFiles);
    }

    // Dangling files (not referenced)
    var allExisting = task.getFileOperations().fileTree(licensesDir);
    allExisting.exclude(
        Arrays.asList(
            "elegant-icon-font-*", // Used by Luke.
            "glove-LICENSE-PDDL.txt" // glove knn dictionary.
            ));

    List<File> dangling = new ArrayList<>(allExisting.getFiles());
    dangling.removeAll(referencedFiles);
    dangling.sort(Comparator.comparing(File::getPath));

    for (File f : dangling) {
      errors.add("Dangling file (not used?): " + f);
    }

    if (!errors.isEmpty()) {
      String message =
          "lucene/licenses folder has validation errors:\n"
              + errors.stream().map(v -> "  - " + v).collect(Collectors.joining("\n"));
      throw new GradleException(message);
    }
  }

  private static void verifyChecksum(
      File licensesDir, JarInfo jarInfo, List<String> errors, List<File> referencedFiles) {
    Path expectedChecksumFile = new File(licensesDir, jarInfo.fileName() + ".sha1").toPath();
    if (!Files.exists(expectedChecksumFile)) {
      errors.add(
          "checksum missing ('" + jarInfo.id() + "'), expected it at: " + expectedChecksumFile);
    } else {
      referencedFiles.add(expectedChecksumFile.toFile());
      String expectedChecksum;
      try {
        expectedChecksum = Files.readString(expectedChecksumFile).trim();
      } catch (IOException e) {
        throw new GradleException("Failed reading checksum file: " + expectedChecksumFile, e);
      }
      String actualChecksum = jarInfo.checksum();
      if (!Objects.equals(expectedChecksum, actualChecksum)) {
        errors.add(
            "checksum mismatch ("
                + jarInfo.id()
                + ", file: "
                + jarInfo.fileName()
                + "), expected it to be: "
                + expectedChecksum
                + ", but was: "
                + actualChecksum);
      }
    }
  }

  private static void inspectLicenseAndNoticeFile(
      CollectJarInfos task,
      File licensesDir,
      JarInfo jarInfo,
      List<String> errors,
      List<File> referencedFiles,
      Pattern licenseNamePattern) {
    // Find license file matching `${baseName}-LICENSE-*.txt` walking left by hyphen.
    List<File> found = new ArrayList<>();
    List<File> candidates = new ArrayList<>();
    String baseName = jarInfo.module();
    while (true) {
      candidates.add(new File(licensesDir, baseName + "-LICENSE-[type].txt"));

      var finalBaseName = baseName;
      var tree = task.getFileOperations().fileTree(licensesDir);
      tree.include(finalBaseName + "-LICENSE-*.txt");

      found.addAll(tree.getFiles());

      String prefix = baseName.replaceAll("[\\-][^-]+$", "");
      if (!found.isEmpty() || prefix.equals(baseName)) {
        break;
      }
      baseName = prefix;
    }

    if (found.isEmpty()) {
      errors.add(
          "License file missing ('"
              + jarInfo.id()
              + "'), expected it at: "
              + candidates.stream().map(File::toString).collect(Collectors.joining(" or "))
              + ", where [type] can be any of "
              + LICENSE_TYPE.keySet()
              + ".");
    } else if (found.size() > 1) {
      errors.add(
          "Multiple license files matching for ('"
              + jarInfo.id()
              + "'): "
              + found.stream().map(File::toString).collect(Collectors.joining(", ")));
    } else {
      File licenseFile = found.getFirst();
      referencedFiles.add(licenseFile);

      Matcher m = licenseNamePattern.matcher(licenseFile.getName());
      if (!m.find()) {
        throw new GradleException(
            "License file name doesn't contain license type?: " + licenseFile.getName());
      }

      String licenseName = m.group(1);
      LicenseType licenseType = LICENSE_TYPE.get(licenseName);
      if (licenseType == null) {
        errors.add(
            "Unknown license type suffix for ('"
                + jarInfo.module()
                + "'): "
                + licenseFile
                + " (must be one of "
                + LICENSE_TYPE.keySet()
                + ")");
      } else {
        // Look for sibling NOTICE file.
        File noticeFile = new File(licenseFile.getPath().replaceAll("-LICENSE-.+", "-NOTICE.txt"));
        if (noticeFile.exists()) {
          referencedFiles.add(noticeFile);
        } else if (!licenseType.noticeOptional()) {
          errors.add(
              "Notice file missing for ('"
                  + jarInfo.module()
                  + "'), expected it at: "
                  + noticeFile);
        }
      }
    }
  }

  /** Update checksums for each jar. */
  private static void writeChecksums(CollectJarInfos task, Directory licensesDir) {
    try {
      for (var jarInfo : task.getUniqueJarInfos()) {
        var expectedChecksumFile =
            licensesDir.file(jarInfo.fileName() + ".sha1").getAsFile().toPath();
        var newChecksum = jarInfo.checksum();
        if (Files.exists(expectedChecksumFile)) {
          var previousChecksum = Files.readString(expectedChecksumFile).trim();
          if (Objects.equals(previousChecksum, newChecksum)) {
            continue;
          }
        }
        Files.writeString(expectedChecksumFile, newChecksum + "\n");
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void configureJarInfoSourceProject(Project project) {
    project
        .getTasks()
        .register(
            "computeJarInfos",
            ComputeJarInfos.class,
            task -> {
              var configurations = project.getConfigurations();
              for (var c :
                  List.of(
                      "runtimeClasspath",
                      "compileClasspath",
                      "testRuntimeClasspath",
                      "testCompileClasspath")) {
                task.include(configurations.named(c));
              }
              task.getOutputFile()
                  .set(project.getLayout().getBuildDirectory().file("jar-deps.json"));
            });
  }
}
