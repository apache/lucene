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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

abstract class HandlerBase implements HttpHandler {

  public static final String MIME_JSON = "text/json";
  public static final String MIME_TEXT = "text/plain";

  void sendResponse(HttpExchange exchange, byte[] responseBytes, String responseContentType)
      throws IOException {
    sendResponse(exchange, 200, responseBytes, responseContentType);
  }

  void sendException(HttpExchange exchange, Exception e) throws IOException {
    sendException(exchange, e.getMessage(), e);
  }

  void sendException(HttpExchange exchange, String message, Exception e) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStreamWriter writer = new OutputStreamWriter(baos, UTF_8);
        PrintWriter printer = new PrintWriter(writer)) {
      printer.println(message);
      e.printStackTrace(printer);
    }
    sendResponse(exchange, 500, baos.toByteArray(), "text/plain");
  }

  void sendResponse(HttpExchange exchange, String response, String responseContentType)
      throws IOException {
    sendResponse(exchange, 200, response.getBytes(UTF_8), responseContentType);
  }

  void sendResponse(
      HttpExchange exchange, int code, byte[] responseBytes, String responseContentType)
      throws IOException {
    // LOG.debug("sending " + responseBytes.length + " bytes as " + responseContentType);
    Headers headers = exchange.getResponseHeaders();
    headers.set("Keep-Alive", "timeout=120 max=120");
    headers.set("Content-Type", responseContentType);
    headers.set("Content-Encoding", "UTF-8");
    writeResponse(exchange, code, responseBytes);
  }

  private static void writeResponse(HttpExchange exchange, int code, byte[] response)
      throws IOException {
    boolean gzip = false;
    List<String> encodings = exchange.getRequestHeaders().get("Accept-Encoding");
    if (encodings != null) {
      OUTER:
      for (String encodingHeader : encodings) {
        for (String encoding : encodingHeader.split(", *", 0)) {
          if ("gzip".equals(encoding)) {
            gzip = true;
            break OUTER;
          }
        }
      }
    }
    if (gzip) {
      Headers headers = exchange.getResponseHeaders();
      headers.set("Content-Encoding", "gzip");
    }

    try (OutputStream httpOut = exchange.getResponseBody()) {
      if (gzip) {
        exchange.sendResponseHeaders(code, 0);
        try (OutputStream out = new GZIPOutputStream(httpOut)) {
          out.write(response, 0, response.length);
        }
      } else {
        exchange.sendResponseHeaders(code, response.length);
        httpOut.write(response, 0, response.length);
      }
    }
  }
}
