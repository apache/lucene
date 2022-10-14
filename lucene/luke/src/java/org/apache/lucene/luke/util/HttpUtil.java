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

package org.apache.lucene.luke.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/** Utilities for handling HTTP requests and responses */
public final class HttpUtil {

  private HttpUtil() {}

  public static Map<String, String> parseQueryString(String queryString) {
    Map<String, String> result = new HashMap<>();
    for (String param : queryString.split("&")) {
      String[] kv = param.split("=", 2);
      if (kv[0].isEmpty()) {
        continue;
      }
      if (result.containsKey(kv[0])) {
        throw new IllegalArgumentException("parameter occurred multiple times: " + param);
      }
      if (kv.length == 1) {
        result.put(kv[0], "");
      } else {
        result.put(kv[0], URLDecoder.decode(kv[1], UTF_8));
      }
    }
    return result;
  }
}
