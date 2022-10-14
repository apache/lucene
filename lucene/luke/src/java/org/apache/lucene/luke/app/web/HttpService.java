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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.models.search.Search;
import org.apache.lucene.luke.models.search.SearchFactory;
import org.apache.lucene.luke.util.LoggerFactory;

public class HttpService {

  private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);

  public static final String LISTENING_MESSAGE = "HTTP service listening on ";

  private final InetSocketAddress sockaddr;
  private final IndexHandler indexHandler;
  private final CountDownLatch tombstone;

  private HttpServer server;

  public HttpService(
      InetSocketAddress sockaddr, IndexHandler indexHandler, CountDownLatch tombstone) {
    this.sockaddr = sockaddr;
    this.indexHandler = indexHandler;
    this.tombstone = tombstone;
  }

  public void start() throws IOException {
    server = HttpServer.create(sockaddr, 2);
    server.createContext("/exit", new ExitHandler());
    Search search = new SearchFactory().newInstance(indexHandler.getState().getIndexReader());
    server.createContext("/search", new SearchHandler(search));
    server.createContext("/overview", new OverviewHandler());
    server.createContext("/terms", new OverviewHandler());
    server.createContext("/ping", new PingHandler());
    server.createContext("/www", new ResourceHandler());
    server.createContext("/", new RedirectHandler("/www/"));
    server.setExecutor(Executors.newFixedThreadPool(2));
    server.start();
    LOG.info(LISTENING_MESSAGE + server.getAddress());
  }

  public void close() {
    ((ThreadPoolExecutor) server.getExecutor()).shutdownNow();
    server.stop(0);
    if (tombstone != null) {
      tombstone.countDown();
    }
  }

  static final class PingHandler extends HandlerBase {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      LOG.info("ping");
      sendResponse(exchange, "OK", MIME_TEXT);
    }
  }

  final class ExitHandler extends HandlerBase {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      LOG.info("exit");
      sendResponse(exchange, "OK", MIME_TEXT);
      close();
    }
  }

  static final class RedirectHandler extends HandlerBase {

    private final String location;

    RedirectHandler(String location) {
      this.location = location;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      exchange.getRequestBody().close();
      String path = exchange.getRequestURI().getPath();
      LOG.info("redirect " + path);
      if (path.charAt(0) == '/') {
        path = path.substring(1);
      }
      Headers headers = exchange.getResponseHeaders();
      headers.set("Location", location + path);
      exchange.sendResponseHeaders(301, 0);
      exchange.getResponseBody().close();
    }
  }
}
