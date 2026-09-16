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
package org.apache.lucene.tests.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

class FieldToType implements BeforeAfterCallback {
  private final Map<String, FieldType> fieldToType = new HashMap<>();

  @Override
  public void after() throws Exception {
    reset();
  }

  synchronized void reset() {
    fieldToType.clear();
  }

  private static Field createField(String name, Object value, FieldType fieldType) {
    if (value instanceof String) {
      return new Field(name, (String) value, fieldType);
    } else if (value instanceof BytesRef) {
      return new Field(name, (BytesRef) value, fieldType);
    } else {
      throw new IllegalArgumentException("value must be String or BytesRef");
    }
  }

  public synchronized Field newField(Random random, String name, Object value, FieldType type) {
    FieldType prevType = fieldToType.get(name);

    if (prevType != null) {
      // always use the same fieldType for the same field name
      return createField(name, value, prevType);
    }

    // TODO: once all core & test codecs can index
    // offsets, sometimes randomly turn on offsets if we are
    // already indexing positions...

    FieldType newType = new FieldType(type);
    if (!newType.stored() && random.nextBoolean()) {
      newType.setStored(true); // randomly store it
    }
    if (newType.indexOptions() != IndexOptions.NONE
        && newType.indexOptions() != IndexOptions.DOCS_AND_CUSTOM_FREQS) {
      if (!newType.storeTermVectors() && random.nextBoolean()) {
        newType.setStoreTermVectors(true);
        if (!newType.storeTermVectorPositions()) {
          newType.setStoreTermVectorPositions(random.nextBoolean());
          if (newType.storeTermVectorPositions()) {
            if (!newType.storeTermVectorPayloads()) {
              newType.setStoreTermVectorPayloads(random.nextBoolean());
            }
          }
        }
        // Check for strings as offsets are disallowed on binary fields
        if (value instanceof String && !newType.storeTermVectorOffsets()) {
          newType.setStoreTermVectorOffsets(random.nextBoolean());
        }

        if (LuceneTestCaseParent.VERBOSE) {
          System.out.println("NOTE: LuceneTestCase: upgrade name=" + name + " type=" + newType);
        }
      }
    }
    newType.freeze();
    fieldToType.put(name, newType);

    // TODO: we need to do this, but smarter, ie, most of
    // the time we set the same value for a given field but
    // sometimes (rarely) we change it up:
    /*
    if (newType.omitNorms()) {
      newType.setOmitNorms(random.nextBoolean());
    }
    */

    return createField(name, value, newType);
  }
}
