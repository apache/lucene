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

/** Query parsers and parsing framework */
module org.apache.lucene.queryparser {
  requires java.xml;
  requires org.apache.lucene.core;
  requires org.apache.lucene.queries;
  requires org.apache.lucene.sandbox;

  exports org.apache.lucene.queryparser.charstream;
  exports org.apache.lucene.queryparser.classic;
  exports org.apache.lucene.queryparser.complexPhrase;
  exports org.apache.lucene.queryparser.ext;
  exports org.apache.lucene.queryparser.flexible.core;
  exports org.apache.lucene.queryparser.flexible.core.builders;
  exports org.apache.lucene.queryparser.flexible.core.config;
  exports org.apache.lucene.queryparser.flexible.core.messages;
  exports org.apache.lucene.queryparser.flexible.core.nodes;
  exports org.apache.lucene.queryparser.flexible.core.parser;
  exports org.apache.lucene.queryparser.flexible.core.processors;
  exports org.apache.lucene.queryparser.flexible.core.util;
  exports org.apache.lucene.queryparser.flexible.messages;
  exports org.apache.lucene.queryparser.flexible.precedence;
  exports org.apache.lucene.queryparser.flexible.precedence.processors;
  exports org.apache.lucene.queryparser.flexible.standard;
  exports org.apache.lucene.queryparser.flexible.standard.builders;
  exports org.apache.lucene.queryparser.flexible.standard.config;
  exports org.apache.lucene.queryparser.flexible.standard.nodes;
  exports org.apache.lucene.queryparser.flexible.standard.nodes.intervalfn;
  exports org.apache.lucene.queryparser.flexible.standard.parser;
  exports org.apache.lucene.queryparser.flexible.standard.processors;
  exports org.apache.lucene.queryparser.simple;
  exports org.apache.lucene.queryparser.surround.parser;
  exports org.apache.lucene.queryparser.surround.query;
  exports org.apache.lucene.queryparser.xml;
  exports org.apache.lucene.queryparser.xml.builders;
}
