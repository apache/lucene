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

import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.luke.util.LoggerFactory;

/** return resources from classpath */
final class ResourceHandler extends HandlerBase {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceHandler.class);

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    LOG.info("GET " + path);
    if (path.charAt(path.length() - 1) == '/') {
      path += "index.html";
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String relativePath = path.substring(1);
    try (InputStream resource = HttpService.class.getResourceAsStream(relativePath)) {
      if (resource == null) {
        LOG.log(Level.SEVERE, "resource " + path + " not found");
        writeResponse(exchange, 404, path + " not found");
        return;
      }
      byte[] buf = new byte[8192];
      int n = resource.read(buf, 0, buf.length);
      while (n > 0) {
        baos.write(buf, 0, n);
        n = resource.read(buf, 0, buf.length);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.toString());
      sendException(exchange, e);
      return;
    }
    sendResponse(exchange, baos.toByteArray(), getContentType(path));
  }

  private String getContentType(String path) {
    int extPos = path.lastIndexOf('.');
    if (extPos == -1) {
      // octet?
      return MIME_TEXT;
    }
    switch (path.substring(extPos + 1)) {
      case "js":
        return "text/javascript";
      case "html":
        return "text/html";
      case "css":
        return "text/css";
      case "png":
        return "image/png";
      case "jpeg":
        return "image/jpeg";
      default:
        return "application/octet-stream";
    }
  }

  private static void writeResponse(HttpExchange exchange, int code, String message)
      throws IOException {
    try (OutputStream httpOut = exchange.getResponseBody()) {
      byte[] response = message.getBytes(UTF_8);
      exchange.sendResponseHeaders(code, response.length);
      httpOut.write(response, 0, response.length);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, e.getMessage());
      throw e;
    }
  }
}
