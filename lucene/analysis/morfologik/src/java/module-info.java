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

/** Analyzer for dictionary stemming, built-in Polish dictionary */
@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.analysis.morfologik {
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;
  requires org.carrot2.morfologik.stemming;
  requires org.carrot2.morfologik.polish;
  requires morfologik.ukrainian.search;

  exports org.apache.lucene.analysis.morfologik;
  exports org.apache.lucene.analysis.uk;

  opens org.apache.lucene.analysis.uk to
      org.apache.lucene.core;

  provides org.apache.lucene.analysis.TokenFilterFactory with
      org.apache.lucene.analysis.morfologik.MorfologikFilterFactory;
}
