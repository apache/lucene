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
package org.apache.lucene.index;

/**
 * Query timeout abstraction that controls whether a query should continue or be stopped. Can be set
 * to the searcher through {@link org.apache.lucene.search.IndexSearcher#setTimeout(QueryTimeout)},
 * in which case bulk scoring will be time-bound. Can also be used in combination with {@link
 * ExitableDirectoryReader}.
 */
public interface QueryTimeout {

  /**
   * Called to determine whether to stop processing a query
   *
   * @return true if the query should stop, false otherwise
   */
  boolean shouldExit();
}
