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
package org.apache.lucene.gradle.plugins.java;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.gitinfo.GitInfoExtension;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.bundling.Jar;

/** Sets up the Jar task in gradle: manifest attributes, extra META-INF files, etc. */
public class JarManifestConfigurationPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);
    Project rootProject = project.getRootProject();

    Path legaleseDir = getProjectRootPath(project);

    Provider<String> gitRevProvider =
        rootProject
            .getExtensions()
            .getByType(GitInfoExtension.class)
            .getGitInfo()
            .getting("git.commit")
            .orElse("-");

    // Declare these inline for now. Don't know if it makes sense to declare them
    // per-project.
    var title = "Lucene Search Engine: " + project.getName();
    var implementationTitle = "org.apache.lucene";

    var globals = rootProject.getExtensions().getByType(LuceneBuildGlobalsExtension.class);

    // Apply the manifest to any JAR or WAR file created by any project.
    project
        .getTasks()
        .withType(Jar.class)
        .configureEach(
            task -> {
              task.getInputs().property("gitRev", gitRevProvider);

              var attributes = new LinkedHashMap<String, Object>();

              attributes.put("Extension-Name", implementationTitle);
              attributes.put("Implementation-Vendor", "The Apache Software Foundation");
              attributes.put("Implementation-Title", implementationTitle);
              attributes.put(
                  "Implementation-Version",
                  getImplementationVersion(project, gitRevProvider.get(), globals));

              // For snapshot builds just include the project version and gitRev so that
              // JARs don't need to be recompiled just because the manifest has changed.
              attributes.put("Specification-Vendor", "The Apache Software Foundation");
              attributes.put("Specification-Version", globals.baseVersion);
              attributes.put("Specification-Title", title);

              var javaPluginExtension =
                  project.getExtensions().getByType(JavaPluginExtension.class);
              attributes.put(
                  "X-Compile-Source-JDK", javaPluginExtension.getSourceCompatibility().toString());
              attributes.put(
                  "X-Compile-Target-JDK", javaPluginExtension.getTargetCompatibility().toString());

              attributes.put(
                  "X-Build-JDK",
                  System.getProperty("java.version")
                      + " ("
                      + System.getProperty("java.vendor")
                      + " "
                      + System.getProperty("java.vm.version")
                      + ")");
              attributes.put(
                  "X-Build-OS",
                  System.getProperty("os.name")
                      + " "
                      + System.getProperty("os.arch")
                      + " "
                      + System.getProperty("os.version"));

              task.getManifest().attributes(attributes);

              // Copy legalese into META-INF.
              task.getMetaInf()
                  .from(
                      legaleseDir.toFile(),
                      spec -> {
                        spec.include("LICENSE.txt", "NOTICE.txt");
                      });
            });
  }

  private static String getImplementationVersion(
      Project project, String gitRev, LuceneBuildGlobalsExtension globals) {
    if (globals.snapshotBuild) {
      return project.getVersion() + " " + gitRev + " [snapshot build, details omitted]";
    } else {
      return project.getVersion()
          + (" " + gitRev)
          + (" - " + globals.buildDate)
          + (" " + globals.buildTime);
    }
  }
}
