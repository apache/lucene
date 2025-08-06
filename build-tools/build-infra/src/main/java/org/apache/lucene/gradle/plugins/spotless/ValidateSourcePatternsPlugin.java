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
package org.apache.lucene.gradle.plugins.spotless;

import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.rat.Defaults;
import org.apache.rat.analysis.RatHeaderAnalysisException;
import org.apache.rat.document.impl.FileDocument;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.internal.logging.progress.ProgressLogger;
import org.gradle.internal.logging.progress.ProgressLoggerFactory;

/** Checks for invalid usage patterns in source files. */
public class ValidateSourcePatternsPlugin extends LuceneGradlePlugin {
  public static final String TASK_NAME = "validateSourcePatterns";

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    project.allprojects(this::configureProject);
  }

  private void configureProject(Project project) {
    var validateSourcePatternsTask =
        project
            .getTasks()
            .register(
                TASK_NAME,
                ValidateSourcePatternsTask.class,
                task -> {
                  task.setGroup("Verification");
                  task.setDescription("Validate Source Patterns");

                  ConfigurableFileTree sourceFiles = task.getSourceFiles();
                  sourceFiles.setDir(project.getLayout().getProjectDirectory());

                  // it seems we only scan XML files - everything else has been moved
                  // to rat scanning or elsewhere.
                  sourceFiles.include("**/*.xml");

                  // default excludes.
                  sourceFiles.exclude("**/build/**");
                  sourceFiles.exclude("**/.idea/**");
                  sourceFiles.exclude("**/.gradle/**");
                  sourceFiles.exclude("**/.git/**");

                  // Don't go into subproject folders (each is scanned individually).
                  sourceFiles.exclude(
                      project.getChildProjects().keySet().stream()
                          .map(name -> name + "/**")
                          .toList());
                });

    // Add to all checks.
    project.getTasks().named("check").configure(task -> task.dependsOn(validateSourcePatternsTask));

    // Ensure validation runs prior to any compilation task.
    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            task -> {
              task.mustRunAfter(validateSourcePatternsTask);
            });

    // project-specific tuning.
    project
        .project(":lucene:benchmark")
        .getTasks()
        .withType(ValidateSourcePatternsTask.class)
        .configureEach(
            task -> {
              task.getSourceFiles().exclude("data/**", "work/**");
            });
  }

  @CacheableTask
  public abstract static class ValidateSourcePatternsTask extends DefaultTask {
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract ConfigurableFileTree getSourceFiles();

    @Inject
    protected abstract ProgressLoggerFactory getProgressLoggerFactory();

    @TaskAction
    public void check() {
      Set<File> files = getSourceFiles().getFiles();
      getLogger()
          .info(
              "Input files for scanning:\n{}",
              files.stream().map(f -> " - " + f).collect(Collectors.joining("\n")));

      var xmlCommentPattern = Pattern.compile("(?sm)\\Q<!--\\E(.*?)\\Q-->\\E");
      var xmlTagPattern = Pattern.compile("(?m)\\s*<[a-zA-Z].*");
      var violations = new TreeSet<String>();

      ProgressLogger progress = getProgressLoggerFactory().newOperation(this.getClass());
      progress.start(this.getName(), this.getName());
      for (var file : files) {
        progress.progress("Scanning " + file.getName());

        String fileText = readUtf8WithValidation(file);

        if (file.getName().endsWith(".xml")) {
          var ratDocument = new FileDocument(file);
          checkLicenseHeaderPrecedes(
              file, "<tag>", xmlTagPattern, xmlCommentPattern, fileText, ratDocument, violations);
        }
      }
      progress.completed();

      if (!violations.isEmpty()) {
        throw new GradleException(
            String.format(
                Locale.ROOT,
                "Found %d source violation(s):\n  %s",
                violations.size(),
                String.join("\n  ", violations)));
      }
    }

    private void checkLicenseHeaderPrecedes(
        File file,
        String description,
        Pattern contentPattern,
        Pattern commentPattern,
        String fileText,
        FileDocument ratDocument,
        TreeSet<String> violations) {
      Matcher contentMatcher = contentPattern.matcher(fileText);
      if (contentMatcher.find()) {
        int contentStartPos = contentMatcher.start();
        Matcher commentMatcher = commentPattern.matcher(fileText);
        while (commentMatcher.find()) {
          if (isLicense(file, commentMatcher.group(1), ratDocument)) {
            if (commentMatcher.start() < contentStartPos) {
              // This file is all good, so break the loop:
              // license header precedes 'description' definition
              break;
            } else {
              reportViolation(
                  violations, file, description + " declaration precedes license header");
            }
          }
        }
      }
    }

    private void reportViolation(TreeSet<String> violations, File file, String name) {
      String msg = String.format(Locale.ROOT, "%s: %s", file, name);
      getLogger().error(msg);
      violations.add(msg);
    }

    // See LUCENE-10419 - rat is not thread safe.
    private static final Object ratLockBug = new Object();
    private static final Splitter lineSplitter = Splitter.on(Pattern.compile("[\\r\\n]+"));

    private boolean isLicense(File file, String text, FileDocument ratDocument) {
      synchronized (ratLockBug) {
        var licenseMatcher = Defaults.createDefaultMatcher();
        licenseMatcher.reset();
        return lineSplitter.splitToList(text).stream()
            .anyMatch(
                it -> {
                  try {
                    return licenseMatcher.match(ratDocument, it);
                  } catch (RatHeaderAnalysisException e) {
                    throw new GradleException("Could not scan this file with rat: " + file, e);
                  }
                });
      }
    }

    private static String readUtf8WithValidation(File file) {
      String fileText;
      CharsetDecoder validatingDecoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      try (var is = Files.newInputStream(file.toPath());
          var sw = new StringWriter();
          var reader = new InputStreamReader(is, validatingDecoder)) {
        reader.transferTo(sw);
        fileText = sw.toString();
      } catch (IOException e) {
        throw new GradleException("Could not read: " + file, e);
      }
      return fileText;
    }
  }
}
