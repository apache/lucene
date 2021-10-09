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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/** The type Search taxis. */
public class SearchTaxis {

  /** Instantiates a new Search taxis. */
  public SearchTaxis() {}

  private static class SearchThread extends Thread {

    /** The Thread id. */
    final int threadID;
    /** The Sparse. */
    final boolean sparse;
    /** The Searcher. */
    final IndexSearcher searcher;
    /** The Iters. */
    final int iters;
    /** The Random. */
    final Random random;
    /** The Results. */
    public final List<String> results = new ArrayList<>();

    /**
     * Instantiates a new Search thread.
     *
     * @param threadID the thread id
     * @param sparse the sparse
     * @param searcher the searcher
     * @param iters the iters
     * @param random the random
     */
    public SearchThread(
        int threadID, boolean sparse, IndexSearcher searcher, int iters, Random random) {
      this.threadID = threadID;
      this.sparse = sparse;
      this.searcher = searcher;
      this.iters = iters;
      this.random = random;
    }

    @Override
    public void run() {
      try {
        _run();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    private void _run() throws IOException {
      for (int i = 0; i < iters; i++) {
        String color;
        String sortField;

        switch (random.nextInt(4)) {
          case 0:
            // TermQuery on yellow cabs
            color = "y";
            if (sparse) {
              sortField = "yellow_pickup_longitude";
            } else {
              sortField = "pickup_longitude";
            }
            break;

          case 1:
            // TermQuery on green cabs
            color = "g";
            if (sparse) {
              sortField = "green_pickup_longitude";
            } else {
              sortField = "pickup_longitude";
            }
            break;

          case 2:
            // BooleanQuery on both cabs (all docs)
            color = "both";
            sortField = null;
            break;

          case 3:
            // Point range query
            color = "neither";
            sortField = null;
            break;

          default:
            throw new AssertionError();
        }

        Query query;
        if (color.equals("both")) {
          BooleanQuery.Builder builder = new BooleanQuery.Builder();
          builder.add(new TermQuery(new Term("cab_color", "y")), BooleanClause.Occur.SHOULD);
          builder.add(new TermQuery(new Term("cab_color", "g")), BooleanClause.Occur.SHOULD);
          query = builder.build();
        } else if (color.equals("neither")) {
          if (sparse) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(
                DoublePoint.newRangeQuery("green_pickup_latitude", 40.75, 40.9),
                BooleanClause.Occur.SHOULD);
            builder.add(
                DoublePoint.newRangeQuery("yellow_pickup_latitude", 40.75, 40.9),
                BooleanClause.Occur.SHOULD);
            query = builder.build();
          } else {
            query = DoublePoint.newRangeQuery("pickup_latitude", 40.75, 40.9);
          }
        } else {
          query = new TermQuery(new Term("cab_color", color));
        }
        Sort sort;
        if (sortField != null && random.nextBoolean()) {
          sort = new Sort(new SortField(sortField, SortField.Type.DOUBLE));
        } else {
          sort = null;
        }

        long t0 = System.nanoTime();
        TopDocs hits;
        if (sort == null) {
          hits = searcher.search(query, 10);
        } else {
          hits = searcher.search(query, 10, sort);
        }
        long t1 = System.nanoTime();
        results.add(
            "T"
                + threadID
                + " "
                + query
                + " sort="
                + sort
                + ": "
                + hits.totalHits
                + " hits in "
                + ((t1 - t0) / 1000000.)
                + " msec");
        for (ScoreDoc hit : hits.scoreDocs) {
          Document doc = searcher.doc(hit.doc);
          results.add("  " + hit.doc + " " + hit.score + ": " + doc.getFields().size() + " fields");
        }

        /*
        synchronized(printLock) {
          System.out.println("T" + threadID + " " + query + " sort=" + sort + ": " + hits.totalHits + " hits in " + ((t1-t0)/1000000.) + " msec");
          for(ScoreDoc hit : hits.scoreDocs) {
            Document doc = searcher.doc(hit.doc);
            System.out.println("  " + hit.doc + " " + hit.score + ": " + doc.getFields().size() + " fields");
          }
        }
        */
      }
    }
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException the io exception
   * @throws InterruptedException the interrupted exception
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    Path indexPath = Paths.get(args[0]);

    String sparseOrNot = args[1];
    boolean sparse;
    if (sparseOrNot.equals("sparse")) {
      sparse = true;
    } else if (sparseOrNot.equals("nonsparse")) {
      sparse = false;
    } else {
      throw new IllegalArgumentException("expected sparse or nonsparse but got: " + sparseOrNot);
    }

    Directory dir = FSDirectory.open(indexPath);

    IndexReader reader = DirectoryReader.open(dir);
    System.out.println("READER: " + reader);

    for (LeafReaderContext ctx : reader.leaves()) {
      CodecReader cr = (CodecReader) ctx.reader();
      System.out.println("\nREADER: " + cr);
    }
    System.out.println("HEAP: 0");

    IndexSearcher searcher = new IndexSearcher(reader);

    Random random = new Random(17);

    SearchThread[] threads = new SearchThread[2];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new SearchThread(i, sparse, searcher, 500, new Random(random.nextLong()));
      threads[i].start();
    }

    for (SearchThread thread : threads) {
      thread.join();
    }

    /*
    SearchThread[] threads = new SearchThread[] {new SearchThread(0, sparse, searcher, 1000, new Random(random.nextLong()))};
    threads[0].run();
    */

    for (SearchThread thread : threads) {
      for (String line : thread.results) {
        System.out.println(line);
      }
    }

    IOUtils.close(reader, dir);
  }
}
