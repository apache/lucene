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
package org.apache.lucene.tests.store;

import java.io.IOException;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.util.TestUtil;

/** Calls check index on close. */
// do NOT make any methods in this class synchronized, volatile
// do NOT import anything from the concurrency package.
// no randoms, no nothing.
public abstract class BaseDirectoryWrapper extends FilterDirectory {

  private boolean checkIndexOnClose = true;
  private int levelForCheckOnClose = CheckIndex.Level.MIN_LEVEL_FOR_SLOW_CHECKS;
  protected volatile boolean isOpen = true;

  protected BaseDirectoryWrapper(Directory delegate) {
    super(delegate);
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      isOpen = false;
      if (checkIndexOnClose && DirectoryReader.indexExists(this)) {
        TestUtil.checkIndex(this, levelForCheckOnClose);
      }
    }
    super.close();
  }

  public boolean isOpen() {
    return isOpen;
  }

  /** Set whether or not checkindex should be run on close */
  public void setCheckIndexOnClose(boolean value) {
    this.checkIndexOnClose = value;
  }

  public boolean getCheckIndexOnClose() {
    return checkIndexOnClose;
  }

  public void setCrossCheckTermVectorsOnClose(boolean value) {
    // If true, we are enabling slow checks.
    if (value == true) {
      this.levelForCheckOnClose = CheckIndex.Level.MIN_LEVEL_FOR_SLOW_CHECKS;
    } else {
      this.levelForCheckOnClose = CheckIndex.Level.MIN_LEVEL_FOR_INTEGRITY_CHECKS;
    }
  }

  public int getLevelForCheckOnClose() {
    return levelForCheckOnClose;
  }
}
