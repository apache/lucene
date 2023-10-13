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
package org.apache.lucene.analysis.ja.dict;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.lucene.util.IOSupplier;

/** Morphological information for unk dictionary. */
final class UnknownMorphData extends TokenInfoMorphData {
  UnknownMorphData(ByteBuffer buffer, IOSupplier<InputStream> posResource) throws IOException {
    super(buffer, posResource);
  }

  @Override
  public String getReading(int morphId, char[] surface, int off, int len) {
    return null;
  }

  @Override
  public String getInflectionType(int morphId) {
    return null;
  }

  @Override
  public String getInflectionForm(int wordId) {
    return null;
  }
}
