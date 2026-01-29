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
import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.gitinfo.GitInfoExtension;
import org.apache.tools.ant.types.selectors.TokenizedPath;
import org.apache.tools.ant.types.selectors.TokenizedPattern;
import org.gradle.api.Project;
import org.gradle.api.specs.Spec;

/** This configures ASL and other license checks. */
public class CheckLicensesPlugin extends LuceneGradlePlugin {
  public static final String CHECK_LICENSES_TASK = "checkLicenses";

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    // Add check licenses task to the top-level project, configure it to scan all files, including
    // those from subprojects. It's fast and simple.
    var checkLicensesTask =
        project
            .getTasks()
            .register(CHECK_LICENSES_TASK, CheckLicensesTask.class, this::configureCheckLicenses);

    // Link any 'check' task from any subproject to this top-level task.
    project.subprojects(
        subproject -> {
          subproject
              .getTasks()
              .named("check")
              .configure(
                  checkTask -> {
                    checkTask.dependsOn(checkLicensesTask);
                  });
        });
  }

  private void configureCheckLicenses(CheckLicensesTask task) {
    Project project = task.getProject();

    assert project.getRootProject() == project;
    GitInfoExtension gitInfoExt = project.getExtensions().getByType(GitInfoExtension.class);

    task.setEnabled(gitInfoExt.getDotGitDir().isPresent());

    var allNonIgnoredFiles = gitInfoExt.getAllNonIgnoredProjectFiles();

    task.getReportFile().set(project.getLayout().getBuildDirectory().file("licenses-report.txt"));

    // Build a list of files excluded from the license check. Just reuse ant's glob patterns.
    var rootDir = getProjectRootPath(project);
    var excludedPaths =
        Stream.of(
                // Ignore binary files. Previously we used apache rat, which had a 'binary guesser'
                // but it's faster to just exclude by name (rather than scan the file).
                "**/*.adoc",
                "**/*.bin",
                "**/*.brk",
                "**/*.bz2",
                "**/*.dat",
                "**/*.gif",
                "**/*.gz",
                "**/*.png",
                "**/*.svg",
                "**/*.xls",
                "**/*.zip",

                // JSON doesn't support comments
                "**/*.json",

                // Ignore build infrastructure and misc utility files.
                ".asf.yaml",
                ".dir-locals.el",
                ".editorconfig",
                ".git-blame-ignore-revs",
                ".gitattributes",
                ".github/**",
                ".gitignore",
                ".lift.toml",
                ".pre-commit-config.yml",
                ".rumdl.toml",
                ".vscode/**",
                "LICENSE.txt",
                "NOTICE.txt",
                "build-options.properties",
                "dev-tools/**",
                "gradle/**",
                "help/*.txt",
                "lucene/licenses/*",
                "versions.lock",

                // Ignore resources in source folders, also generated resources.
                "**/src/**/*.txt",
                "**/src/**/*.properties",
                "**/src/**/*.utf8",
                "**/src/generated/**",

                // Ignore other binary resources within sources. Try to be
                // specific here.
                "build-tools/build-infra-shadow/src/java/keep.me",
                "lucene/CHANGES.txt",
                "lucene/analysis.tests/src/**/*.aff",
                "lucene/analysis.tests/src/**/*.dic",
                "lucene/analysis/common/src/**/*.aff",
                "lucene/analysis/common/src/**/*.dic",
                "lucene/analysis/common/src/**/*.good",
                "lucene/analysis/common/src/**/*.htm*",
                "lucene/analysis/common/src/**/*.rslp",
                "lucene/analysis/common/src/**/*.sug",
                "lucene/analysis/common/src/**/*.wrong",
                "lucene/analysis/icu/src/**/utr30.nrm",
                "lucene/analysis/kuromoji/src/**/bocchan.utf-8",
                "lucene/analysis/morfologik/src/**/*.dict",
                "lucene/analysis/morfologik/src/**/*.info",
                "lucene/analysis/morfologik/src/**/*.input",
                "lucene/analysis/opennlp/src/**/en-test-lemmas.dict",
                "lucene/analysis/smartcn/src/**/*.mem",
                "lucene/analysis/stempel/src/**/*.tbl",
                "lucene/benchmark/.gitignore",
                "lucene/demo/src/**/knn-token-vectors",
                "lucene/luke/src/**/ElegantIcons.ttf",
                "lucene/test-framework/src/**/europarl.lines.txt.seek",

                // these may require a review, actually?
                "lucene/queryparser/docs/**",
                "lucene/benchmark/conf/*",
                "lucene/benchmark/README.enwiki")
            .map(TokenizedPattern::new)
            .toList();

    // I thought it'd be possible to somehow precompile those glob filters but apparently not.
    // I guess it's fine if the list of patterns and files is of reasonable size.
    Spec<File> maybeExcludeKnownExceptions =
        file -> {
          // relativize from root project directory and normalize path separators.
          var filePath = normalizePathSeparators(rootDir.relativize(file.toPath()).toString());
          return excludedPaths.stream()
              .noneMatch(pattern -> pattern.matchPath(new TokenizedPath(filePath), true));
        };
    task.getFiles().from(project.files(allNonIgnoredFiles).filter(maybeExcludeKnownExceptions));
  }

  private String normalizePathSeparators(String path) {
    if (File.separatorChar == '\\') {
      return path.replace(File.separatorChar, '/');
    } else {
      return path;
    }
  }
}
