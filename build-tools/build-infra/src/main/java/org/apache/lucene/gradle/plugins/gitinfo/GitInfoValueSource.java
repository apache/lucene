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
package org.apache.lucene.gradle.plugins.gitinfo;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.gradle.api.GradleException;
import org.gradle.api.provider.ValueSource;
import org.gradle.process.ExecOperations;

public abstract class GitInfoValueSource
    implements ValueSource<Map<String, String>, GitValueSourceParameters> {

  @Inject
  public abstract ExecOperations getExecOps();

  @Override
  public Map<String, String> obtain() {
    if (!getParameters().getDotDir().isPresent()) {
      return Map.ofEntries(
          Map.entry("git.commit", "unknown"),
          Map.entry("git.commit-short", "unknown"),
          Map.entry("git.clean", "false"),
          Map.entry("git.changed-files", "not a git checkout"));
    }

    try (var baos = new ByteArrayOutputStream();
        var out = new BufferedOutputStream(baos)) {
      var result =
          getExecOps()
              .exec(
                  spec -> {
                    spec.setStandardOutput(out);
                    spec.setErrorOutput(out);
                    spec.setIgnoreExitValue(true);

                    getParameters().configure(spec);

                    spec.setExecutable(getParameters().getGitExec().get());
                    spec.args("status", "--porcelain=v2", "--branch");
                  });
      out.flush();

      // Just assume it's UTF-8. I don't know if this can break anyhow.
      String gitOutput = baos.toString(StandardCharsets.UTF_8);

      if (result.getExitValue() != 0) {
        // Something went wrong. Assume this isn't a git checkout and
        // add placeholder values.
        return Map.ofEntries(
            Map.entry("git.commit", "unknown"),
            Map.entry("git.commit-short", "unknown"),
            Map.entry("git.clean", "false"),
            Map.entry("git.changed-files", "not a checkout?"),
            Map.entry("git.error-log", gitOutput));
      }

      // Parse git-porcelain output.
      Map<String, String> properties = new TreeMap<>();

      Pattern headerLine = Pattern.compile("# (?<key>[^ ]+) (?<value>.+)");
      Pattern changeLine = Pattern.compile("(?<type>[12u?!]) (?<value>.+)");
      StringBuilder changedLines = new StringBuilder();
      for (var line : gitOutput.split("\\n")) {
        if (line.startsWith("#")) {
          Matcher matcher = headerLine.matcher(line);
          if (matcher.matches()) {
            String key = matcher.group("key");
            String value = matcher.group("value");
            properties.put(key, value);

            if (key.equals("branch.oid")) {
              properties.put("git.commit", value);
              properties.put("git.commit-short", value.substring(0, 8));
            }
          }
        } else if (changeLine.matcher(line).matches()) {
          changedLines.append(line);
          changedLines.append("\n");
        } else {
          // Omit other lines.
        }
      }

      properties.put("git.changed-files", changedLines.toString());
      properties.put("git.clean", changedLines.isEmpty() ? "true" : "false");

      return properties;
    } catch (IOException e) {
      throw new GradleException("Errors calling git to fetch local repository status.", e);
    }
  }
}
