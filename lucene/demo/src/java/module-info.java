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

/** Simple example code for Apache Lucene */
module org.apache.lucene.demo {
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;
  requires org.apache.lucene.facet;
  requires org.apache.lucene.queries;
  requires org.apache.lucene.queryparser;
  requires org.apache.lucene.expressions;
  requires org.apache.lucene.sandbox;

<<<<<<<< HEAD:buildSrc/scriptDepVersions.gradle
ext {
  scriptDepVersions = [
      "apache-rat": "0.14",
      "asm": "9.7",
      "commons-codec": "1.13",
      "ecj": "3.30.0",
      "flexmark": "0.61.24",
      "javacc": "7.0.12",
      "jflex": "1.8.2",
      "jgit": "5.13.1.202206130422-r",
  ]
========
  exports org.apache.lucene.demo;
  exports org.apache.lucene.demo.facet;
  exports org.apache.lucene.demo.knn;
>>>>>>>> origin/branch_10_1:lucene/demo/src/java/module-info.java
}
