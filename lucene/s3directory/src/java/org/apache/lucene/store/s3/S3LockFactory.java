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
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.s3.client.HttpMethod;
import org.apache.lucene.store.s3.client.xml.XmlElement;
import org.apache.lucene.store.s3.client.xml.builder.Xml;

/** An AWS S3 lock factory implementation based on legal holds. */
public final class S3LockFactory extends LockFactory {

  /** Singleton instance */
  public static final S3LockFactory INSTANCE = new S3LockFactory();

  /** Default constructor. */
  private S3LockFactory() {}

  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (dir instanceof S3Directory s3Directory) {
      return new S3LegalHoldLock(s3Directory, lockName);
    } else {
      return NoLockFactory.INSTANCE.obtainLock(dir, lockName);
    }
  }

  static final class S3LegalHoldLock extends Lock {

    private S3Directory s3Directory;
    private String name;

    S3LegalHoldLock(final S3Directory s3Directory, final String name) throws IOException {
      this.s3Directory = s3Directory;
      this.name = name;
      obtain();
    }

    public void obtain() throws LockObtainFailedException {
      try {
        putObjectLegalHold("ON");
      } catch (Exception e) {
        throw new LockObtainFailedException("Lock object could not be created: ", e);
      }
    }

    @Override
    public void close() throws AlreadyClosedException {
      try {
        putObjectLegalHold("OFF");
      } catch (Exception e) {
        throw new AlreadyClosedException("Lock was already released: ", e);
      }
    }

    @Override
    public void ensureValid() throws AlreadyClosedException {
      try {
        if (!isLegalHoldOn()) {
          throw new AlreadyClosedException("Lock instance already released: " + this);
        }
      } catch (
          @SuppressWarnings("unused")
          Exception e) {
        initializeLockFile();
      }
    }

    private boolean isLegalHoldOn() {
      XmlElement res =
          s3Directory
              .getS3()
              .path(s3Directory.getBucket(), s3Directory.getPath() + name)
              .query("legal-hold")
              .method(HttpMethod.GET)
              .responseAsXml();
      return res != null && res.hasChildren() && res.child("Status").content().equals("ON");
    }

    private void putObjectLegalHold(String status) throws NoSuchAlgorithmException {
      String body =
          Xml.create("LegalHold")
              .a("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
              .e("Status")
              .content(status)
              .toString();
      try {
        s3Directory
            .getS3()
            .path(s3Directory.getBucket(), s3Directory.getPath() + name)
            .query("legal-hold")
            .header("Content-MD5", S3Directory.md5AsBase64(body.getBytes(StandardCharsets.UTF_8)))
            .method(HttpMethod.PUT)
            .requestBody(body)
            .execute();
      } catch (
          @SuppressWarnings("unused")
          Exception e) {
        initializeLockFile();
      }
    }

    private void initializeLockFile() {
      if (!s3Directory.fileExists(name)) {
        // initialize the write.lock file immediately after bucket creation
        s3Directory
            .getS3()
            .path(s3Directory.getBucket(), s3Directory.getPath() + IndexWriter.WRITE_LOCK_NAME)
            .method(HttpMethod.PUT)
            .requestBody("")
            .execute();
      }
    }

    @Override
    public String toString() {
      return "S3LegalHoldLock[" + s3Directory.getBucket() + "/" + name + "]";
    }
  }
}
