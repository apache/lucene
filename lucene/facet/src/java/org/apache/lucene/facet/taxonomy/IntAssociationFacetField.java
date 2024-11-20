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
package org.apache.lucene.facet.taxonomy;

import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Add an instance of this to your {@link Document} to add a facet label associated with an int. Use
 * {@link TaxonomyFacetIntAssociations} to aggregate int values per facet label at search time.
 *
 * @lucene.experimental
 */
public class IntAssociationFacetField extends AssociationFacetField {

  /** Creates this from {@code dim} and {@code path} and an int association */
  public IntAssociationFacetField(int assoc, String dim, String... path) {
    super(intToBytesRef(assoc), dim, path);
  }

  /** Encodes an {@code int} as a 4-byte {@link BytesRef}, big-endian. */
  public static BytesRef intToBytesRef(int v) {
    byte[] bytes = new byte[4];
    // big-endian:
    BitUtil.VH_BE_INT.set(bytes, 0, v);
    return new BytesRef(bytes);
  }

  /** Decodes a previously encoded {@code int}. */
  public static int bytesRefToInt(BytesRef b) {
    return (int) BitUtil.VH_BE_INT.get(b.bytes, b.offset);
  }

  @Override
  public String toString() {
    return "IntAssociationFacetField(dim="
        + dim
        + " path="
        + Arrays.toString(path)
        + " value="
        + bytesRefToInt(assoc)
        + ")";
  }
}
