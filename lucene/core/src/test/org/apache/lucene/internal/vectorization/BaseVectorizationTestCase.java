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
package org.apache.lucene.internal.vectorization;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;

public abstract class BaseVectorizationTestCase extends LuceneTestCase {

  protected static final VectorizationProvider LUCENE_PROVIDER = new DefaultVectorizationProvider();
  protected static final VectorizationProvider PANAMA_PROVIDER = VectorizationProvider.lookup(true);

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeTrue(
        "Test only works when JDK's vector incubator module is enabled.",
        PANAMA_PROVIDER.getClass() != LUCENE_PROVIDER.getClass());
  }
}
