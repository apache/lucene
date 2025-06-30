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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

/** Registers global build constants and extensions. */
public class RegisterBuildGlobalsPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);
    requiresAppliedPlugin(project, BuildOptionsPlugin.class);

    String luceneVersion = getLuceneVersion(project);
    project.setVersion(luceneVersion);

    var tstamp = ZonedDateTime.now();
    String buildDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(tstamp);
    String buildTime = DateTimeFormatter.ofPattern("HH:mm:ss").format(tstamp);
    String buildYear = DateTimeFormatter.ofPattern("yyyy").format(tstamp);

    String baseVersion = getBaseVersion(luceneVersion);
    project.allprojects(
        p -> {
          var globals =
              p.getExtensions()
                  .create(LuceneBuildGlobalsExtension.NAME, LuceneBuildGlobalsExtension.class);
          globals.baseVersion = baseVersion;
          globals.snapshotBuild = luceneVersion.contains("SNAPSHOT");
          globals.buildDate = buildDate;
          globals.buildTime = buildTime;
          globals.buildYear = buildYear;
        });
  }

  /**
   * The "base" version is stripped of the qualifier. Compute it because somebody might have passed
   * -Pversion.release=x.y.z directly.
   */
  private String getBaseVersion(String luceneVersion) {
    var matcher =
        Pattern.compile("^(?<baseVersion>\\d+\\.\\d+\\.\\d+)(-(.+))?").matcher(luceneVersion);
    if (!matcher.matches()) {
      throw new GradleException("Can't strip version to just x.y.z: " + luceneVersion);
    }
    return matcher.group("baseVersion");
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
