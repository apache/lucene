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
package org.apache.lucene.gradle.plugins.misc;

import nl.littlerobots.vcu.plugin.VersionCatalogFormatTask;
import nl.littlerobots.vcu.plugin.VersionCatalogUpdateExtension;
import nl.littlerobots.vcu.plugin.VersionCatalogUpdatePlugin;
import nl.littlerobots.vcu.plugin.VersionCatalogUpdateTask;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.tools.ant.taskdefs.FixCRLF;
import org.gradle.api.Project;

/** Apply cleanups to {@code gradle/libs.gradle.toml}. */
public class CleanupVersionsFilePlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    rootProject.getPlugins().apply(VersionCatalogUpdatePlugin.class);

    // apply and configure the version catalog update plugin.
    var versionCatalogUpdate =
        rootProject.getExtensions().getByType(VersionCatalogUpdateExtension.class);
    versionCatalogUpdate.getSortByKey().set(true);

    // Hook up version catalog formatting to tidy.
    var tasks = rootProject.getTasks();
    var formatDepsTask =
        tasks.withType(VersionCatalogFormatTask.class).named("versionCatalogFormat");

    // correct crlf and the default encoding after version
    // catalog formatting finishes. It is a pity it cannot be specified as the
    // task's parameters.
    formatDepsTask.configure(
        task -> {
          task.doLast(
              _ -> {
                var fixcrlf = (FixCRLF) task.getAnt().getAntProject().createTask("fixcrlf");
                fixcrlf.setFile(task.getCatalogFile().getAsFile().get());
                fixcrlf.setFixlast(true);
                var lf = new FixCRLF.CrLf();
                lf.setValue("lf");
                fixcrlf.setEol(lf);
                fixcrlf.setOutputEncoding("UTF-8");
                fixcrlf.execute();
              });
        });

    tasks
        .named("tidy")
        .configure(
            task -> {
              task.dependsOn(formatDepsTask);
            });

    // Configure versionCatalogUpdate task to be interactive (not automatically overwrite).
    var updateDepsTask =
        tasks.withType(VersionCatalogUpdateTask.class).named("versionCatalogUpdate");

    updateDepsTask.configure(
        task -> {
          task.setInteractive(true);
        });
  }
}
