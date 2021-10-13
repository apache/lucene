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
import java.nio.file.Paths;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

/** The type Open close index writer. */
public class OpenCloseIndexWriter {

  /** Instantiates a new Open close index writer. */
  public OpenCloseIndexWriter() {}

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException the io exception
   */
  public static void main(String[] args) throws IOException {
    final String dirPath = args[0];
    final Directory dir = new MMapDirectory(Paths.get(dirPath));
    final Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    final IndexWriterConfig iwc = new IndexWriterConfig(a);
    final IndexWriter writer = new IndexWriter(dir, iwc);
    // System.out.println("Segments: " + writer.segString());
    writer.close();
    dir.close();
  }
}
