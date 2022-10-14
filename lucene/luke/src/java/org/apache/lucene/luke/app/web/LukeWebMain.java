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

package org.apache.lucene.luke.app.web;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.util.LoggerFactory;

/** Entry class for web Luke */
public final class LukeWebMain {

  private LukeWebMain() {
  }

  static {
    LoggerFactory.initGuiLogging();
  }

  public static void main(String[] args) throws Exception {
    Map<String, Object> parsed = null;
    try {
      parsed = parseArgs(args);
    } catch (Exception e) {
      usage(e.getMessage());
    }
    IndexHandler indexHandler = IndexHandler.getInstance();
    indexHandler.open(getIndex(parsed), "org.apache.lucene.store.FSDirectory", true, true, false);
    CountDownLatch tombstone = new CountDownLatch(1);
    HttpService httpService = new HttpService(getSockAddr(parsed), indexHandler, tombstone);
    httpService.start();
    tombstone.await();
  }

  private static String getIndex(Map<String, Object> args) {
    String index = (String) args.get("index");
    if (index == null) {
      usage("index arg is required");
    }
    return index;
  }

  private static InetSocketAddress getSockAddr(Map<String, Object> args) {
    String host = (String) args.get("host");
    int port = (Integer) args.getOrDefault("port", 8080);
    if (host == null) {
      return new InetSocketAddress(port);
    } else {
      return new InetSocketAddress(host, port);
    }
  }

  private static Map<String, Object> parseArgs(String[] args) {
    HashMap<String, Object> parsed = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--index":
          parsed.put("index", args[++i]);
          break;
        case "--port":
          parsed.put("port", Integer.parseInt(args[++i]));
          break;
        case "--host":
          parsed.put("host", args[++i]);
          break;
        default:
          usage("unknown arg: " + args[i]);
      }
    }
    return parsed;
  }

  private static void usage(String message) {
    System.err.println(message + "; usage: LukeWebMain --index <path-to-index> [--port <port>] [--host <host>]");
    Runtime.getRuntime().exit(1);
  }
}
