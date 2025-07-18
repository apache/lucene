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
package org.apache.lucene.gradle.plugins.globals;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsPlugin;
import com.carrotsearch.randomizedtesting.SeedUtils;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

/** Registers global build constants and extensions. */
public class RegisterBuildGlobalsPlugin extends LuceneGradlePlugin {
  private static final Pattern VERSION_PATTERN =
      Pattern.compile("^(?<baseVersion>(?<majorVersion>\\d+)\\.\\d+\\.\\d+)(-(.+))?");

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);
    requiresAppliedPlugin(project, BuildOptionsPlugin.class);

    String luceneVersion = getLuceneVersion(project);
    project.setVersion(luceneVersion);

    var tstamp = ZonedDateTime.now();
    String buildDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT).format(tstamp);
    String buildTime = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).format(tstamp);
    String buildYear = DateTimeFormatter.ofPattern("yyyy", Locale.ROOT).format(tstamp);

    String baseVersion = getBaseVersion(luceneVersion);
    String majorVersion = getMajorVersion(luceneVersion);

    boolean isCIBuild =
        System.getenv().keySet().stream()
            .anyMatch(key -> key.matches("(?i)((JENKINS|HUDSON)(_\\w+)?|CI)"));

    // Set up build options for external tools.
    var buildOptions = getBuildOptions(project);
    buildOptions.addOption(
        "lucene.tool.python3", "External python3 executable (path or name)", "python3");
    buildOptions.addOption("lucene.tool.perl", "External perl executable (path or name)", "perl");
    buildOptions.addOption("lucene.tool.git", "External git executable (path or name)", "git");

    // Pick the "root" seed from which everything else that is randomized is derived.
    Provider<String> rootSeedOption =
        getBuildOptions(project)
            .addOption(
                "tests.seed",
                "The \"root\" randomization seed for options and test parameters.",
                project.provider(
                    () -> String.format(Locale.ROOT, "%08X", new Random().nextLong())));
    String rootSeed = rootSeedOption.get();

    // We take just the root seed, ignoring any chained sub-seeds.
    long rootSeedLong = SeedUtils.parseSeedChain(rootSeed)[0];

    // Parse the minimum Java version required to run Lucene.
    JavaVersion minJavaVersion =
        JavaVersion.toVersion(getVersionCatalog(project).findVersion("minJava").get().toString());

    boolean isIdea = Boolean.parseBoolean(System.getProperty("idea.active", "false"));
    boolean isIdeaSync = Boolean.parseBoolean(System.getProperty("idea.sync.active", "false"));
    boolean isIdeaBuild = (isIdea && !isIdeaSync);

    project.allprojects(
        p -> {
          var globals =
              p.getExtensions()
                  .create(LuceneBuildGlobalsExtension.NAME, LuceneBuildGlobalsExtension.class);
          globals.baseVersion = baseVersion;
          globals.majorVersion = majorVersion;
          globals.snapshotBuild = luceneVersion.contains("SNAPSHOT");
          globals.buildDate = buildDate;
          globals.buildTime = buildTime;
          globals.buildYear = buildYear;
          globals.isCIBuild = isCIBuild;
          globals.getRootSeed().set(rootSeed);
          globals.getRootSeedAsLong().set(rootSeedLong);
          globals
              .getProjectSeedAsLong()
              .convention(rootSeedLong ^ p.getPath().hashCode())
              .finalizeValue();
          globals.getMinJavaVersion().convention(minJavaVersion).finalizeValue();
          globals
              .getIntellijIdea()
              .convention(new IntellijIdea(isIdea, isIdeaSync, isIdeaBuild))
              .finalizeValue();
        });
  }

  /** The "major" version in semantic versioning scheme. */
  private String getMajorVersion(String luceneVersion) {
    return parseVersion(luceneVersion).group("majorVersion");
  }

  /**
   * The "base" version is stripped of the qualifier. Compute it because somebody might have passed
   * -Pversion.release=x.y.z directly.
   */
  private String getBaseVersion(String luceneVersion) {
    return parseVersion(luceneVersion).group("baseVersion");
  }

  private static Matcher parseVersion(String luceneVersion) {
    var matcher = VERSION_PATTERN.matcher(luceneVersion);
    if (!matcher.matches()) {
      throw new GradleException("Can't strip version to just x.y.z: " + luceneVersion);
    }
    return matcher;
  }

  /**
   * Figure out project version strings based on "version.base" and "version.suffix" properties or
   * use "version.release" if provided explicitly.
   */
  private static String getLuceneVersion(Project project) {
    var buildOptions = project.getExtensions().getByType(BuildOptionsExtension.class);
    Provider<String> versionBase = buildOptions.addOption("version.base", "Base Lucene version");
    Provider<String> versionSuffix =
        buildOptions.addOption(
            "version.suffix", "Appends project version suffix to 'version.base'.", "SNAPSHOT");
    Provider<String> versionProvider =
        buildOptions.addOption(
            "version.release",
            "Lucene project version.",
            versionSuffix.map(suffix -> versionBase.get() + "-" + suffix).orElse(versionBase));
    String luceneVersion = versionProvider.get();
    return luceneVersion;
  }
}
