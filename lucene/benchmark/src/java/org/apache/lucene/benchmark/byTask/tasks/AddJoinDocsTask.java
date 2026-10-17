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
import org.apache.lucene.document.Document;

/**
 * Adds the next {@link JoinDocMaker} product group via {@link
 * org.apache.lucene.index.IndexWriter#addDocuments}, which keeps a block join block contiguous.
 */
public class AddJoinDocsTask extends PerfTask {

  private List<Document> group;

  public AddJoinDocsTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    group = ((JoinDocMaker) getRunData().getDocMaker()).makeGroup();
  }

  @Override
  public int doLogic() throws Exception {
    getRunData().getIndexWriter().addDocuments(group);
    return group.size();
  }

  @Override
  public void tearDown() throws Exception {
    group = null;
    super.tearDown();
  }
}
