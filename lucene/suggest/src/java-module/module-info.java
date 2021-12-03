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

/** Auto-suggest and Spellchecking support */
module org.apache.lucene.suggest {
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;

  exports org.apache.lucene.search.spell;
  exports org.apache.lucene.search.suggest;
  exports org.apache.lucene.search.suggest.analyzing;
  exports org.apache.lucene.search.suggest.document;
  exports org.apache.lucene.search.suggest.fst;
  exports org.apache.lucene.search.suggest.tst;

  provides org.apache.lucene.codecs.PostingsFormat with
      org.apache.lucene.search.suggest.document.Completion50PostingsFormat,
      org.apache.lucene.search.suggest.document.Completion84PostingsFormat,
      org.apache.lucene.search.suggest.document.Completion90PostingsFormat;
  provides org.apache.lucene.analysis.TokenFilterFactory with
      org.apache.lucene.search.suggest.analyzing.SuggestStopFilterFactory;
}
