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
package org.apache.lucene.facet.facetset;

import org.apache.lucene.util.BytesRef;

/**
 * A functional interface for decoding facet set values. You can use it by e.g. implementing a
 * static method with the same signature and then pass it as {@code YourClass::decode}.
 */
public interface FacetSetDecoder {

  /**
   * Decodes the facet set dimension values into the given destination buffer and returns the number
   * of bytes read.
   */
  int decode(BytesRef bytesRef, int start, long[] dest);
}
