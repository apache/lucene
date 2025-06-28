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

package org.apache.lucene.gradle.validation;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;

public abstract class JarLicenseValidationTask extends JarValidationTask {

  // All known license types. If 'noticeOptional' is true then
  // the notice file must accompany the license.
  public static final Map<String, LicenseType> LICENSE_TYPES;

  static {
    Map<String, LicenseType> tmp = new LinkedHashMap<>();
    tmp.put("ASL", new LicenseType("Apache Software License 2.0"));
    tmp.put("BSD", new LicenseType("Berkeley Software Distribution"));
    // BSD-like: someone tweaked the BSD license to add their own copyright, etc.
    tmp.put("BSD_LIKE", new LicenseType("BSD like license"));
    tmp.put(
        "CDDL",
        new LicenseType("Common Development and Distribution License", /*noticeOptional*/ true));
    tmp.put("CPL", new LicenseType("Common Public License"));
    tmp.put("EPL", new LicenseType("Eclipse Public License 1.0", true));
    tmp.put("MIT", new LicenseType("Massachusetts Institute of Tech. License", true));
    tmp.put("MPL", new LicenseType("Mozilla Public License", true /* not sure */));
    tmp.put("PD", new LicenseType("Public Domain", true));
    tmp.put("PDDL", new LicenseType("Public Domain Dedication and License", true));
    tmp.put("SUN", new LicenseType("Sun Open Source License", true));
    tmp.put("COMPOUND", new LicenseType("Compound license (details in NOTICE file)."));

    LICENSE_TYPES = Map.copyOf(tmp); // make it unmodifiable
  }

  @InputDirectory
  public abstract RegularFileProperty getLicenseDir();

  @Inject
  public JarLicenseValidationTask() {
    setGroup("Dependency validation");
    setDescription("Validate license and notice files of dependencies");
  }

  @TaskAction
  void validateJar() {
    List<String> errors = new ArrayList<>();
    File licensesDir = getLicenseDir().get().getAsFile();

    jarInfos.forEach(
        dep -> {
          var baseName = dep.getName();
          List<File> found = new ArrayList<>();
          List<File> candidates = new ArrayList<>();
          while (true) {
            candidates.add(new File(licensesDir, baseName + "-LICENSE-[type].txt"));
            found.addAll(
                getFileOperations()
                    .fileTree(Map.of("dir", licensesDir, "include", baseName + "-LICENSE-*.txt"))
                    .getFiles());
            String prefix = baseName.replaceAll("[\\\\-][^-]+\\$", "");
            if (found.size() > 0 || prefix == baseName) {
              break;
            }
            baseName = prefix;
          }

          if (found.size() == 0) {
            errors.add(
                "License file missing ('"
                    + dep.getModule()
                    + "'), expected it at: "
                    + candidates.stream().map(File::getPath).collect(Collectors.joining(" or "))
                    + " where [type] can be any of ${licenseTypes.keySet()}.");
          } else if (found.size() > 1) {
            errors.add(
                "Multiple license files matching for ('"
                    + dep.getModule()
                    + "')): "
                    + found.stream().map(File::getPath).collect(Collectors.joining(", ")));
          } else {
            File licenseFile = found.get(0);
            dep.addReferencedFile(licenseFile);
            Pattern pattern = Pattern.compile("LICENSE-(.+)\\.txt$");
            Matcher matcher = pattern.matcher(licenseFile.getName());
            if (!matcher.find()) {
              throw new GradleException(
                  "License file name doesn't contain license type?: " + licenseFile.getName());
            }

            String licenseName = matcher.group(1);
            LicenseType licenseType = LICENSE_TYPES.get(licenseName);

            if (licenseType == null) {
              errors.add(
                  "Unknown license type suffix for ('"
                      + dep.getModule()
                      + "'): "
                      + licenseFile.getPath()
                      + " (must be one of "
                      + LICENSE_TYPES.keySet()
                      + ")");
            } else {
              getLogger()
                  .info(
                      String.format(
                          "Dependency license file OK ('%s'): " + licenseName, dep.getModule()));

              // Look for sibling NOTICE file.
              File noticeFile =
                  new File(
                      licenseFile
                          .getPath() // same directory as the LICENSE file
                          .replaceAll("-LICENSE-.+", "-NOTICE.txt")); // LICENSE-… → NOTICE.txt

              if (noticeFile.exists()) {
                dep.addReferencedFile(noticeFile);
                getLogger()
                    .info("Dependency notice file OK ('%s'): " + noticeFile, dep.getModule());
              } else if (!licenseType.isNoticeOptional()) {
                errors.add(
                    String.format(
                        "Notice file missing for ('%s'), expected it at: %s",
                        dep.getModule(), noticeFile));
              }
            }
          }
        });
    if (errors.size() > 0) {
      String msg =
          "Certain license/ notice files are missing:\n  - "
              + errors.stream().collect(Collectors.joining("\n  - "));
      // TODO: make this a verification task to support optional failOnError
      throw new GradleException(msg);
    }
  }

  @Inject
  protected abstract FileOperations getFileOperations();
}
