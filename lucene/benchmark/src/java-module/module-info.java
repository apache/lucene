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
@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.benchmark {
  requires java.xml;
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;
  requires org.apache.lucene.facet;
  requires org.apache.lucene.highlighter;
  requires org.apache.lucene.queries;
  requires org.apache.lucene.queryparser;
  requires org.apache.lucene.spatial_extras;
  requires spatial4j;

  exports org.apache.lucene.benchmark;
  exports org.apache.lucene.benchmark.byTask;
  exports org.apache.lucene.benchmark.byTask.feeds;
  exports org.apache.lucene.benchmark.byTask.programmatic;
  exports org.apache.lucene.benchmark.byTask.stats;
  exports org.apache.lucene.benchmark.byTask.tasks;
  exports org.apache.lucene.benchmark.byTask.utils;
  exports org.apache.lucene.benchmark.quality;
  exports org.apache.lucene.benchmark.quality.trec;
  exports org.apache.lucene.benchmark.quality.utils;
  exports org.apache.lucene.benchmark.utils;
}
