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
 * Execute exact phrase queries using the PIM system.
 */
public class SearchWikiDPU {

  private static final int NB_THREADS=64;

  private SearchWikiDPU() { }

  private static OperatingSystemMXBean mbean =
    (com.sun.management.OperatingSystemMXBean)
    ManagementFactory.getOperatingSystemMXBean();

  public static void main(String[] args) throws Exception {
    String usage =
      "Usage:\tjava SearchWikiDPU [-index dir] [-field f] [-queries file]\n";
    if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
      System.out.println(usage);
      System.exit(0);
    }

    String index = "index";
    String field = "contents";
    String queries = null;
    long totalTime = 0;
    long cpuTime = 0;
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
      } else if ("-ntopdocs".equals(args[i])) {
        try {
          nb_topdocs = Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException e) {
          System.out.println("Error: wrong number of top docs.");
          break;
        }
        i++;
      }
    }

    IndexReader reader = DirectoryReader.open(MMapDirectory.open(Paths.get(index)));
    IndexSearcher searcher = new IndexSearcher(reader);

    // load PIM index from PIM directory
    PimSystemManager.setNumAllocDpus(1024);
    if(PimSystemManager.get().loadPimIndex(MMapDirectory.open(Paths.get(index + "/dpu"))))
      System.out.println("Loaded PIM index with " + PimSystemManager.get().getNbDpus() + " DPUs");
    else
      System.out.println("WARNING: failed to load PIM Index");

    BufferedReader in = null;
    if (queries != null) {
      in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
    } else {
      System.out.println(usage);
      System.exit(0);
    }

    boolean first = true;
    int nbReq = 0;
    while (true) {

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
      PimPhraseQuery query = builder.build().setMaxNumHitsFromDpuSystem(nb_topdocs);
      System.out.println("Searching for: " + query.toString(field));

      long start = System.nanoTime();
      long cpuStart = mbean.getProcessCpuTime();
      TopDocs results = searcher.search(query, 100);
      long end = System.nanoTime();
      // ignore first request as its latency is not representative due to cold caches
      if (!first) {
        totalTime += (end - start);
        cpuTime += (mbean.getProcessCpuTime() - cpuStart);
        nbReq++;
      }
      System.out.println("Time: " + String.format("%.2f", (end - start) * 1e-6) + "ms");
      int numTotalHits = Math.toIntExact(results.totalHits.value);
      System.out.println(numTotalHits + " total matching documents");

      StoredFields storedFields = reader.storedFields();
      ScoreDoc[] hits = results.scoreDocs;
      int nbRes = Math.min(numTotalHits, Math.min(nb_topdocs, 5));
      for (int i = 0; i < nbRes; i++) {

        System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);

        Document doc = storedFields.document(hits[i].doc);
        String path = doc.get("path");
        if (path != null) {
          System.out.println((i + 1) + ". " + path);
          String title = doc.get("title");
          if (title != null) {
            System.out.println("   Title: " + doc.get("title"));
          }
        } else {
          System.out.println((i + 1) + ". " + "No path for this document");
        }
      }
      first = false;
    }

    reader.close();
    PimSystemManager.get().shutDown();

    System.out.println("Total time: " + String.format("%.2f", totalTime * 1e-6)
        + " ms" + ", CPU Time: " + (double) cpuTime / 1e6 + " ms" + ", Nb req.: " + nbReq);
  }
}
