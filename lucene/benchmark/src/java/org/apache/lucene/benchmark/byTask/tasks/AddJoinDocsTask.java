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
package org.apache.lucene.benchmark.byTask.tasks;

import java.util.List;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.Dataset;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

/**
 * Adds the next {@link JoinDocMaker} product group via {@link IndexWriter#addDocuments}, which
 * keeps a block join block contiguous, and records the {@link Dataset} in the commit user data.
 */
public class AddJoinDocsTask extends PerfTask {

  private List<Document> group;
  private IndexWriter writer;

  public AddJoinDocsTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    JoinDocMaker docMaker = (JoinDocMaker) getRunData().getDocMaker();
    group = docMaker.makeGroup();
    IndexWriter current = getRunData().getIndexWriter();
    if (current != writer) {
      writer = current;
      writer.setLiveCommitData(docMaker.getDataset().toUserData().entrySet());
    }
  }

  @Override
  public int doLogic() throws Exception {
    writer.addDocuments(group);
    return group.size();
  }

  @Override
  public void tearDown() throws Exception {
    group = null;
    super.tearDown();
  }
}
