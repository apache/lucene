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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.FSDirectory;

/**
 * Search program based on Lucene's demo example.
 * Execute exact phrase queries.
 */
public class SearchWiki {

    private SearchWiki() {
    }

    private static OperatingSystemMXBean mbean =
            (com.sun.management.OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean();

    /**
     * Simple command-line based search demo.
     */
    public static void main(String[] args) throws Exception {
        String usage =
                "Usage:\tjava org.apache.lucene.demo.SearchWiki [-index dir] [-field f] [-queries file] [-query string] " +
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
        ExecutorService executor = Executors.newFixedThreadPool(40);
        IndexSearcher searcher = new IndexSearcher(reader, executor);

        BufferedReader in = null;
        if (queries != null) {
            in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
        } else {
            in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        }

        boolean first = true;
        int nbReq = 0;
        while (true) {
            if (queries == null && queryString == null) { // prompt the user
                System.out.println("Enter query: ");
            }

            String line = queryString != null ? queryString : in.readLine();

            if (line == null || line.length() == -1) {
                break;
            }

            line = line.trim();
            if (line.length() == 0) {
                break;
            }

            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            String[] words = line.split(" ");
            int wid = 0;
            for (String word : words) {
                builder.add(new Term("contents", word), wid++);
            }
            PhraseQuery query = builder.build();
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
            int nbRes = Math.min(numTotalHits, 5);
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

            if (queryString != null) {
                break;
            }
            first = false;
        }
        reader.close();
        executor.shutdown();
        System.out.println("Total time: " + String.format("%.2f", totalTime * 1e-6) + " ms" + ", CPU Time: " +
                (double) cpuTime / 1e6 + " ms" + ", Nb req.: " + nbReq);
    }
}
