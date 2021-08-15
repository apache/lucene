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

package org.apache.lucene.search;

import java.io.IOException;

/**
 * Defines an entity that can be positioned on a specified document. It may not be able to determine
 * a valid set of documents it can be positioned on on its own, and may not be able to determine a
 * "next" document, but can determine whether-or-not it can be positioned on a specified document.
 */
public abstract class DocIdPositionable {

  /**
   * Advance this instance to the given document id
   *
   * @return true if there is a value for this document
   */
  public abstract boolean advanceExact(int doc) throws IOException;
}
