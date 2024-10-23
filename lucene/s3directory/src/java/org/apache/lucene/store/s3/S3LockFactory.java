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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NoLockFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;

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

    public void obtain() throws IOException {
      try {
        putObjectLegalHold(ObjectLockLegalHoldStatus.ON);
      } catch (AwsServiceException | SdkClientException e) {
        throw new LockObtainFailedException("Lock object could not be created: ", e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        putObjectLegalHold(ObjectLockLegalHoldStatus.OFF);
      } catch (AwsServiceException | SdkClientException e) {
        throw new AlreadyClosedException("Lock was already released: ", e);
      }
    }

    @Override
    public void ensureValid() {
      try {
        if (!isLegalHoldOn()) {
          throw new AlreadyClosedException("Lock instance already released: " + this);
        }
      } catch (Exception e) {
        throw new AlreadyClosedException("Lock object not found: " + this, e);
      }
    }

    private boolean isLegalHoldOn() {
      return s3Directory
          .getS3()
          .getObjectLegalHold(
              b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPath() + name))
          .legalHold()
          .status()
          .equals(ObjectLockLegalHoldStatus.ON);
    }

    private void putObjectLegalHold(ObjectLockLegalHoldStatus status) {
      s3Directory
          .getS3()
          .putObjectLegalHold(
              PutObjectLegalHoldRequest.builder()
                  .bucket(s3Directory.getBucket())
                  .key(s3Directory.getPath() + name)
                  .legalHold(ObjectLockLegalHold.builder().status(status).build())
                  .build());
    }

    @Override
    public String toString() {
      return "S3LegalHoldLock[" + s3Directory.getBucket() + "/" + name + "]";
    }
  }
}
