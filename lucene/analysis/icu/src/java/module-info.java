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

/** Analysis integration with ICU */
@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.analysis.icu {
  requires com.ibm.icu;
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;

  exports org.apache.lucene.analysis.icu;
  exports org.apache.lucene.analysis.icu.segmentation;
  exports org.apache.lucene.analysis.icu.tokenattributes;

  opens org.apache.lucene.analysis.icu.segmentation to
      org.apache.lucene.core;

  provides org.apache.lucene.analysis.CharFilterFactory with
      org.apache.lucene.analysis.icu.ICUNormalizer2CharFilterFactory;
  provides org.apache.lucene.analysis.TokenizerFactory with
      org.apache.lucene.analysis.icu.segmentation.ICUTokenizerFactory;
  provides org.apache.lucene.analysis.TokenFilterFactory with
      org.apache.lucene.analysis.icu.ICUFoldingFilterFactory,
      org.apache.lucene.analysis.icu.ICUNormalizer2FilterFactory,
      org.apache.lucene.analysis.icu.ICUTransformFilterFactory;
}
