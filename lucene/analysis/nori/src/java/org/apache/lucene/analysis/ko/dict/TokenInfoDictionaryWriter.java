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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.lucene.analysis.morph.BinaryDictionaryWriter;
import org.apache.lucene.util.fst.FST;

class TokenInfoDictionaryWriter extends BinaryDictionaryWriter<TokenInfoDictionary> {
  private FST<Long> fst;

  TokenInfoDictionaryWriter(int size) {
    super(TokenInfoDictionary.class, new TokenInfoDictionaryEntryWriter(size));
  }

  public void setFST(FST<Long> fst) {
    Objects.requireNonNull(fst, "dictionary must not be empty");
    this.fst = fst;
  }

  @Override
  protected void addMapping(int sourceId, int wordId) {
    super.addMapping(sourceId, wordId);
  }

  @Override
  public void write(Path baseDir) throws IOException {
    super.write(
        baseDir,
        DictionaryConstants.TARGETMAP_HEADER,
        DictionaryConstants.POSDICT_HEADER,
        DictionaryConstants.DICT_HEADER,
        DictionaryConstants.VERSION);
    writeFST(baseDir.resolve(getBaseFileName() + TokenInfoDictionary.FST_FILENAME_SUFFIX));
  }

  private void writeFST(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    fst.save(path);
  }
}
