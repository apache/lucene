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
package org.apache.lucene.analysis.cn.smart;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Manages analysis data configuration for SmartChineseAnalyzer
 *
 * <p>SmartChineseAnalyzer has a built-in dictionary and stopword list out-of-box.
 *
 * @lucene.experimental
 */
public class AnalyzerProfile {

  /** Global indicating the configured analysis data directory */
  public static final String ANALYSIS_DATA_DIR = resolveDataDir();

  private static String resolveDataDir() {
    String dirName = "analysis-data";
    String propName = "analysis.properties";

    // Try the system property：-Danalysis.data.dir=/path/to/analysis-data
    String analysisDataDir = System.getProperty("analysis.data.dir", "");
    if (analysisDataDir.isEmpty() == false) return analysisDataDir;

    Path lib = Paths.get("lib");
    Path[] candidateFiles =
        new Path[] {
          Paths.get(dirName), lib.resolve(dirName), Paths.get(propName), lib.resolve(propName)
        };
    for (Path file : candidateFiles) {
      if (Files.exists(file)) {
        if (Files.isDirectory(file)) {
          analysisDataDir = file.toAbsolutePath().toString();
        } else if (Files.isRegularFile(file) && getAnalysisDataDir(file).isEmpty() == false) {
          analysisDataDir = getAnalysisDataDir(file);
        }
        break;
      }
    }

    if (analysisDataDir.isEmpty()) {
      // Dictionary directory cannot be found.
      throw new RuntimeException(
          "WARNING: Can not find lexical dictionary directory!"
              + " This will cause unpredictable exceptions in your application!"
              + " Please refer to the manual to download the dictionaries.");
    }
    return analysisDataDir;
  }

  private static String getAnalysisDataDir(Path propFile) {
    Properties prop = new Properties();
    try (BufferedReader reader = Files.newBufferedReader(propFile, StandardCharsets.UTF_8)) {
      prop.load(reader);
      return prop.getProperty("analysis.data.dir", "");
    } catch (IOException _) {
      return "";
    }
  }
}
