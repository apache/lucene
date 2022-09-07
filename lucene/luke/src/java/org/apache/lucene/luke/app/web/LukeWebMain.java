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

import java.util.concurrent.CountDownLatch;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.util.LoggerFactory;

/** Entry class for web Luke */
public class LukeWebMain {

  static {
    LoggerFactory.initGuiLogging();
  }

  public static void main(String[] args) throws Exception {
    String index = null;
    if (args.length == 2 && args[0].equals("--index")) {
      index = args[1];
    } else {
      System.err.println("usage: LukeWebMain --index <path-to-index>");
      Runtime.getRuntime().exit(1);
    }

    IndexHandler indexHandler = IndexHandler.getInstance();
    indexHandler.open(index, "org.apache.lucene.store.FSDirectory", true, true, false);
    CountDownLatch tombstone = new CountDownLatch(1);
    HttpService httpService = new HttpService(indexHandler, tombstone);
    httpService.start();
    tombstone.await();
  }
}
