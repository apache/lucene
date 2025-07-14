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
package org.apache.lucene.gradle.plugins.hacks;

import de.undercouch.gradle.tasks.download.Download;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;

/** This applies various odd hacks that we probably should not need. */
public class HacksPlugin extends LuceneGradlePlugin {

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              applyRetryDownloads(project);
            });
  }

  private void applyRetryDownloads(Project project) {
    project
        .getTasks()
        .withType(Download.class)
        .configureEach(
            task -> {
              task.retries(3);
            });
  }
}
