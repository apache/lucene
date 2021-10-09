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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.HdrHistogram.Histogram;

// javac -cp lib/HdrHistogram.jar ToHGRM.java

/** The type To hgrm. */
public class ToHGRM {

  /** Instantiates a new To hgrm. */
  public ToHGRM() {}

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    double maxSec = Double.parseDouble(args[0]);

    BufferedReader br =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    Histogram h = new Histogram((long) (maxSec * 1000), 3);
    while (true) {
      String line = br.readLine();
      if (line == null) {
        break;
      }
      h.recordValue((long) (1000 * Double.parseDouble(line.trim())));
    }

    h.outputPercentileDistribution(System.out, 50, 1.0);
  }
}
