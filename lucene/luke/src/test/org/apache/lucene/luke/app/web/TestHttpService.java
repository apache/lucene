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
import static org.apache.lucene.luke.util.HttpUtil.parseQueryString;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.luke.util.JsonUtil;
import org.apache.lucene.luke.util.CircularLogBufferHandler;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestRuleLimitSysouts;
import org.junit.After;
import org.junit.BeforeClass;
import org.xml.sax.InputSource;

/**
 * Tests of Luke webapp
 *
 * <p>Note that we rely on JS/Ajax to load many of the page components, so we can't see that using
 * simple HTTP tests, but we can also test the underlying data calls.
 */
@TestRuleLimitSysouts.Limit(bytes = 16000) // HttpService logs stuff to sysout by design
public class TestHttpService extends LuceneTestCase {

  // TODO: use a random free port and discover it
  private static final String URL_PREFIX = "http://localhost:";
  private static XPath xpath;
  private static DocumentBuilder documentBuilder;

  private int servicePort;
  private boolean serviceStarted;

  @BeforeClass
  public static void init() throws Exception {
    xpath = XPathFactory.newInstance().newXPath();
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setExpandEntityReferences(false);
    documentBuilder = dbf.newDocumentBuilder();
  }

  @After
  public void cleanup() throws Exception {
    if (serviceStarted) {
      stop();
    }
  }

  public void testService() throws Exception {
    start(createTestIndex());
  }

  public void testOverview() throws Exception {
    start(createTestIndex());
    // check HTML content of template
    org.w3c.dom.Document html = httpGetHtml("/");
    assertEquals("Index Overview", xpath.evaluate("/html/body/h1", html));

    // check response to /overview data call
    Map<?, ?> response = (Map<?, ?>) httpGetJson("/overview");
    Map<?, ?> summary = (Map<?, ?>) response.get("summary");
    assertEquals(1d, summary.get("number of fields"));
    assertEquals(1d, summary.get("number of documents"));
    assertEquals(5d, summary.get("number of terms"));
    assertEquals(0d, summary.get("number of deletions"));
    Map<?, ?> fieldTermCount = (Map<?, ?>) response.get("term_fields");
    assertEquals(Map.of("text", 5d), fieldTermCount);
    Map<?, ?> fieldDocCount = (Map<?, ?>) response.get("field_doc_count");
    assertEquals(Map.of("text", 1d), fieldDocCount);

    // check response to /terms data call
    List<?> termsResponse = (List<?>) httpGetJson("/terms?field=text");
    assertEquals(5, termsResponse.size());
    // order is nondeterministic
    Map<Object, Object> terms = new HashMap<>();
    for (Object item : termsResponse) {
      Map<?, ?> term = (Map<?, ?>) item;
      terms.put(term.get("decoded_term_text"), term.get("doc_freq"));
    }
    assertEquals(Map.of("my", 1d, "life", 1d, "is", 1d, "a", 1d, "dog", 1d), terms);
  }

  public void testSearch() throws Exception {
    start(createTestIndex());
    org.w3c.dom.Document html = httpGetHtml("/www/search.html");
    assertEquals("Search", xpath.evaluate("/html/body/h1", html));
    assertEquals("Lucene Search", xpath.evaluate("/html/head/title", html));

    Map<?, ?> response = (Map<?, ?>) httpGetJson("/search?q=life");
    assertEquals(0d, response.get("start"));
    assertEquals(1d, response.get("total_hits"));
    assertEquals("text:life", response.get("parsed_query"));
    List<?> hits = (List<?>) response.get("hits");
    assertEquals(1, hits.size());
    Map<?, ?> hit = (Map<?, ?>) hits.get(0);
    assertEquals(0d, hit.get("doc_id"));
    assertEquals(Set.of("doc_id", "score", "field_values"), hit.keySet());
    // we didn't index any stored fields
    assertEquals(Map.of(), hit.get("field_values"));
  }

  public void testParseQueryString() {
    assertEquals(Map.of("a", "b"), parseQueryString("a=b"));
    assertEquals(Map.of("a", "b c"), parseQueryString("a=b%20c"));
    assertEquals(Map.of("a", "b c"), parseQueryString("a=b c"));
    assertEquals(Map.of("a", "b c"), parseQueryString("a=b+c"));
    assertEquals(Map.of("a", "b=c"), parseQueryString("a=b=c"));
    assertEquals(Map.of("b", "c"), parseQueryString("=x&b=c"));
    assertEquals(Map.of("a", "b", "b", "c"), parseQueryString("a=b&b=c"));
    assertEquals(Map.of("a", "", "b", ""), parseQueryString("a&b&"));
    expectThrows(IllegalArgumentException.class, () -> parseQueryString("a=1&a=2"));
  }

  public void testNotFound() throws Exception {
    start(createTestIndex());
    IOException e = expectThrows(FileNotFoundException.class, () -> httpGet("/blahblahblah/"));
    assertTrue(e.getMessage(), e.getMessage().contains("/blahblahblah"));
  }

  private void stop() throws IOException {
    httpGet("/exit");
  }

  private static Path createTestIndex() throws IOException {
    Path indexPath = createTempDir("index-TestHttpService");
    try (Directory dir = newFSDirectory(indexPath);
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new TextField("text", "my life is a dog", Field.Store.YES));
      writer.addDocument(doc);
    }
    return indexPath;
  }

  @SuppressWarnings("unused")
  private void start(Path indexPath) throws Exception {
    Thread serviceThread = new Thread(
            () -> {
              try {
                LukeWebMain.main(new String[] {"--index", indexPath.toString(),
                                               "--host", "127.0.0.1",
                                               "--port", "0"});
              } catch (Exception e) {
                fail("caught exception: " + e);
              }
            });
    serviceThread.start();
    serviceStarted = true;
    for (int retries = 5; retries > 0; retries--) {
      try {
        if (LoggerFactory.circularBuffer == null) {
          Thread.sleep(10);
          continue;
        }
        for (CircularLogBufferHandler.ImmutableLogRecord logRecord : LoggerFactory.circularBuffer.getLogRecords()) {
          // wait for log message telling which port the service is listening on
          String message = logRecord.getMessage();
          if (message.startsWith(HttpService.LISTENING_MESSAGE)) {
            System.out.println(message);
            servicePort = Integer.parseInt(message.substring(message.lastIndexOf(':') + 1));
          }
        }
        if (servicePort == -1) {
          Thread.sleep(20);
          continue;
        }
        httpGet("/ping");
      } catch (IOException e) {
        Thread.sleep(200);
        continue;
      }
      break;
    }
    if (servicePort == -1) {
      serviceThread.stop();
    }
  }

  private org.w3c.dom.Document httpGetHtml(String path) throws Exception {
    // ignores all headers; assumes UTF-8 text
    String text = httpGetText(path);
    InputSource inputSource = new InputSource(new StringReader(text));
    try {
      return documentBuilder.parse(inputSource);
    } catch (Exception e) {
      throw new IllegalStateException("failed to parse content of " + path + ":\n" + text, e);
    }
  }

  private Object httpGetJson(String path) throws Exception {
    return JsonUtil.parse(httpGetText(path));
  }

  private String httpGetText(String path) throws IOException {
    // ignores all headers; assumes UTF-8 text
    return new String(httpGet(path), UTF_8);
  }

  private byte[] httpGet(String path) throws IOException {
    URLConnection conn = new URL(URL_PREFIX + servicePort + path).openConnection();
    ((HttpURLConnection) conn).setRequestMethod("GET");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    try (InputStream in = conn.getInputStream()) {
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
    }
    return baos.toByteArray();
  }
}
