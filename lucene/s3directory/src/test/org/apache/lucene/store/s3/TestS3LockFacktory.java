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
package org.apache.lucene.store.s3;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;

public class TestS3LockFacktory extends LuceneTestCase {

  private S3Directory dir1;
  private S3Directory dir2;

  @Override
  public void setUp() {
    dir1 = new S3Directory(TestS3Directory.TEST_BUCKET, "");
    dir1.create();

    dir2 = new S3Directory(TestS3Directory.TEST_BUCKET1, "");
  }

  @Override
  public void tearDown() {}

  @Test
  public void testLocks() throws Exception {
    try (Lock lock1 = dir1.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
      lock1.ensureValid();
      try {
        dir2.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        Assert.fail("lock2 should not have valid lock");
      } catch (final IOException e) {
        Logger.getLogger(TestS3LockFacktory.class.getName()).log(Level.SEVERE, null, e);
      }
    } finally {
      dir1.close();
      dir1.emptyBucket();
      dir1.delete();
      dir2.close();
    }
  }
}
