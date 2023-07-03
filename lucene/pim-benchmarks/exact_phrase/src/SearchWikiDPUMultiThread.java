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
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.sandbox.pim.PimPhraseQuery;
import org.apache.lucene.sandbox.pim.PimSystemManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.FSDirectory;

/**
 * Search program based on Lucene's demo example.
 * Search are done using the PIM system and multiple threads
 * are using a single IndexSearcher to execute phrase queries in parallel.
 */
public class SearchWikiDPUMultiThread {

  private static final int NB_THREADS=8;

  private SearchWikiDPUMultiThread() {}

  private static OperatingSystemMXBean mbean =
        (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String usage =
        "Usage:\tjava org.apache.lucene.demo.SearchWikiDPU [-index dir] [-field f] [-queries file] [-query string] " +
                "\n\nSee http://lucene.apache.org/core/4_1_0/demo/ for details.";
    if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
      System.out.println(usage);
      System.exit(0);
    }

    String index = "index";
    String field = "contents";
    String queries = null;
    int repeat = 0;
    String queryString = null;
    int totalTime = 0;
    long cpuTime = 0;

    for (int i = 0; i < args.length; i++) {
      if ("-index".equals(args[i])) {
        index = args[i + 1];
        i++;
      } else if ("-field".equals(args[i])) {
        field = args[i + 1];
        i++;
      } else if ("-queries".equals(args[i])) {
        queries = args[i + 1];
        i++;
      } else if ("-query".equals(args[i])) {
        queryString = args[i + 1];
        i++;
      }
    }

    IndexReader reader = DirectoryReader.open(MMapDirectory.open(Paths.get(index)));
    //ExecutorService executor = Executors.newFixedThreadPool(40);
    //IndexSearcher searcher = new IndexSearcher(reader, executor);
    IndexSearcher searcher = new IndexSearcher(reader);

    // load PIM index from PIM directory
    PimSystemManager.get().loadPimIndex(MMapDirectory.open(Paths.get(index + "/dpu")));

    System.out.println("Loaded PIM index with " + PimSystemManager.get().getNbDpus() + " DPUs");

    BufferedReader in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
    int lines = 0;
    while (in.readLine() != null) lines++;
    in.close();

    System.out.println("Starting " + NB_THREADS + " threads for index search");
    BufferedReader[] readers = new BufferedReader[NB_THREADS];
    SearchTask[] searchTasks = new SearchTask[NB_THREADS];
    Thread[] threads = new Thread[NB_THREADS];
    for(int i = 0; i < NB_THREADS; ++i) {
      readers[i] =  Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
      searchTasks[i] = new SearchTask(i, searcher, reader, field, readers[i],
              i * (lines / NB_THREADS),
              i == (NB_THREADS - 1) ? lines - (lines / NB_THREADS * (NB_THREADS - 1)) : lines / NB_THREADS);
      threads[i] = new Thread(searchTasks[i]);
      threads[i].start();
    }

    for(int i = 0; i < NB_THREADS; ++i) {
      threads[i].join();
    }
    for(int i = 0; i < NB_THREADS; ++i) {
      System.out.println("THREAD " + i + ":");
      System.out.println(searchTasks[i].out.toString());
    }

    reader.close();
    //executor.shutdown();
    PimSystemManager.get().shutDown();
  }

  private static class SearchTask implements Runnable {

    private final int id;
    private BufferedReader in;
    private final int nbLines;
    private ByteArrayOutputStream out;
    private IndexSearcher searcher;
    private IndexReader reader;
    private String field;
    int totalTime = 0;
    long cpuTime = 0;
    int nbReq = 0;

    SearchTask(int id, IndexSearcher searcher, IndexReader reader, String field,
               BufferedReader in, int firstLine, int nbLines) {
      this.id  = id;
      this.searcher = searcher;
      this.reader = reader;
      this.field = field;
      this.in = in;
      this.nbLines = nbLines;
      this.out = new ByteArrayOutputStream();

      // skip the first lines
      try {
        for (int i = 0; i < firstLine; ++i)
          in.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() {

      boolean first = true;
      try {
        for (int l = 0; l < nbLines; ++l) {

          String line = in.readLine();

          if (line == null || line.length() == -1) {
            break;
          }

          line = line.trim();
          if (line.length() == 0) {
            break;
          }

          PimPhraseQuery.Builder builder = new PimPhraseQuery.Builder();
          String[] words = line.split(" ");
          int wid = 0;
          for (String word : words) {
            builder.add(new Term("contents", word), wid++);
          }
          PimPhraseQuery query = builder.build();
          out.writeBytes(new String("Searching for: " + query.toString(field) + "\n").getBytes());

          long start = System.nanoTime();
          //TODO Make a version without print and count total time
          long cpuStart = mbean.getProcessCpuTime();
          // TODO try 10/100/1000
          TopDocs results = searcher.search(query, 100);
          long end = System.nanoTime();
          // ignore first request as its latency is not representative due to cold caches
          if (!first) {
            totalTime += (System.nanoTime() - start);
            cpuTime += (mbean.getProcessCpuTime() - cpuStart);
            nbReq++;
          }
          out.writeBytes(new String("Time: " + String.format("%.2f", (System.nanoTime() - start) * 1e-6) + "ms" + "\n").getBytes());
          int numTotalHits = Math.toIntExact(results.totalHits.value);
          out.writeBytes(new String(numTotalHits + " total matching documents" + "\n").getBytes());


          StoredFields storedFields = reader.storedFields();
          ScoreDoc[] hits = results.scoreDocs;
          int nbRes = Math.min(numTotalHits, 5);
          for (int i = 0; i < nbRes; i++) {

            out.writeBytes(new String("doc=" + hits[i].doc + " score=" + hits[i].score + "\n").getBytes());

            Document doc = storedFields.document(hits[i].doc);
            String path = doc.get("path");
            if (path != null) {
              out.writeBytes(new String((i + 1) + ". " + path + "\n").getBytes());
              String title = doc.get("title");
              if (title != null) {
                out.writeBytes(new String("   Title: " + doc.get("title") + "\n").getBytes());
              }
            } else {
              out.writeBytes(new String((i + 1) + ". " + "No path for this document" + "\n").getBytes());
            }
          }
          first = false;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      out.writeBytes(new String("Total time: " + String.format("%.2f", totalTime * 1e-6) + " ms" + ", CPU Time: " +
              (double)cpuTime / 1e6 + " ms" + ", Nb req.: " + nbReq + "\n").getBytes());
    }
  }
}
