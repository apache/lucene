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
import java.io.IOException;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
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

  private static final int NB_THREADS=64;

  private SearchWikiDPUMultiThread() {}

  private static OperatingSystemMXBean mbean =
        (com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String usage =
        "Usage:\tjava SearchWikiDPUMultiThread [-index dir] [-field f] [-queries file]\n";
    if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
      System.out.println(usage);
      System.exit(0);
    }

    String index = "index";
    String field = "contents";
    String queries = null;
    long cpuTime = 0;
    int nb_threads = 64;
    int nb_topdocs = 100;

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
      } else if ("-nthreads".equals(args[i])) {
        try {
          nb_threads = Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException e) {
          System.out.println("Error: wrong number of threads.");
          System.exit(1);
        }
        i++;
      } else if ("-ntopdocs".equals(args[i])) {
        try {
          nb_topdocs = Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException e) {
          System.out.println("Error: wrong number of top docs.");
          System.exit(1);
        }
        i++;
      }
    }

    IndexReader reader = DirectoryReader.open(MMapDirectory.open(Paths.get(index)));
    //ExecutorService executor = Executors.newFixedThreadPool(64);
    //IndexSearcher searcher = new IndexSearcher(reader, executor);
    IndexSearcher searcher = new IndexSearcher(reader);

    // load PIM index from PIM directory
    PimSystemManager.setNumAllocDpus(2048);
    if(PimSystemManager.get().loadPimIndex(MMapDirectory.open(Paths.get(index + "/dpu"))))
      System.out.println("Loaded PIM index with " + PimSystemManager.get().getNbDpus() + " DPUs");
    else {
      System.out.println("WARNING: failed to load PIM Index");
      System.exit(1);
    }

    BufferedReader in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
    int nb_queries = 0;
    while (in.readLine() != null) nb_queries++;
    in.close();

    System.out.println("Starting " + nb_threads + " threads for index search");
    BufferedReader[] readers = new BufferedReader[nb_threads];
    SearchTask[] searchTasks = new SearchTask[nb_threads];
    Thread[] threads = new Thread[nb_threads];
    for(int i = 0; i < nb_threads; ++i) {
      readers[i] =  Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
      searchTasks[i] = new SearchTask(i, searcher, reader, field, readers[i],
              i * (nb_queries / nb_threads),
              i == (nb_threads - 1) ? nb_queries - (nb_queries / nb_threads * (nb_threads - 1)) : nb_queries / nb_threads,
              nb_topdocs);
      threads[i] = new Thread(searchTasks[i]);
    }

    long start = System.nanoTime();
    for(int i = 0; i < nb_threads; ++i) {
      threads[i].start();
    }

    for(int i = 0; i < nb_threads; ++i) {
      threads[i].join();
    }
    long end = System.nanoTime();
    long threadTime = 0, threadMaxTime = 0;
    int nbReq = 0;
    for(int i = 0; i < nb_threads; ++i) {
      System.out.println("THREAD " + i + ":");
      System.out.println(searchTasks[i].out.toString());
      threadTime += searchTasks[i].totalTime;
      if(threadMaxTime < searchTasks[i].totalTime)
        threadMaxTime = searchTasks[i].totalTime;
      nbReq += searchTasks[i].nbReq;
    }

    System.out.println("Cumulative time: " + String.format("%.2f", (end - start) * 1e-6) 
        + " ms, #queries=" + nb_queries + " throughput=" 
        + String.format("%.2f", ((double)nb_queries * 1e9 / (end - start))) + " (queries/sec)");
    System.out.println("Thread cumulative time = " + String.format("%.2f", threadTime * 1e-6) + "ms, nbRreq=" + nbReq + " time/req=" +
        String.format("%.2f", threadTime * 1e-6 / nbReq) +
        " Thread max time = " + String.format("%.2f", threadMaxTime * 1e-6));

    reader.close();
    PimSystemManager.get().shutDown();
  }

  private static class SearchTask implements Runnable {

    private final int id;
    private BufferedReader in;
    private final int nbLines;
    private final int nb_topdocs;
    private ByteArrayOutputStream out;
    private IndexSearcher searcher;
    private IndexReader reader;
    private String field;
    long totalTime = 0;
    long cpuTime = 0;
    int nbReq = 0;

    SearchTask(int id, IndexSearcher searcher, IndexReader reader, String field,
               BufferedReader in, int firstLine, int nbLines, int nb_topdocs) {
      this.id  = id;
      this.searcher = searcher;
      this.reader = reader;
      this.field = field;
      this.in = in;
      this.nbLines = nbLines;
      this.out = new ByteArrayOutputStream();
      this.nb_topdocs = nb_topdocs;

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
          PimPhraseQuery query = builder.build();//.setMaxNumHitsFromDpuSystem(nb_topdocs);
          out.writeBytes(new String("Searching for: " + query.toString(field) + "\n").getBytes());

          //TODO Make a version without print and count total time
          long cpuStart = mbean.getProcessCpuTime();
          // TODO try 10/100/1000
          long start = System.nanoTime();
          TopDocs results = searcher.search(query, nb_topdocs);
          long end = System.nanoTime();
          totalTime += (end - start);
          cpuTime += (mbean.getProcessCpuTime() - cpuStart);
          nbReq++;
          out.writeBytes(new String("Time: " + String.format("%.2f", (System.nanoTime() - start) * 1e-6) + "ms" + "\n").getBytes());
          int numTotalHits = Math.toIntExact(results.totalHits.value);
          out.writeBytes(new String(numTotalHits + " total matching documents" + "\n").getBytes());


          StoredFields storedFields = reader.storedFields();
          ScoreDoc[] hits = results.scoreDocs;
          int nbRes = Math.min(numTotalHits, Math.min(nb_topdocs, 5));
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
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      out.writeBytes(new String("Total time: " + String.format("%.2f", totalTime * 1e-6) + " ms" + ", CPU Time: " +
              (double)cpuTime / 1e6 + " ms" + ", Nb req.: " + nbReq + "\n").getBytes());
    }
  }
}
