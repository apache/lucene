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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ThreadInterruptedException;

/** Test utility - slow directory */
// TODO: move to test-framework and sometimes use in tests?
public class SlowDirectory extends FilterDirectory {

  private static final int IO_SLEEP_THRESHOLD = 50;

  Random random;
  private int sleepMillis;

  public void setSleepMillis(int sleepMillis) {
    this.sleepMillis = sleepMillis;
  }

  public SlowDirectory(int sleepMillis, Random random) {
    super(new ByteBuffersDirectory());
    this.sleepMillis = sleepMillis;
    this.random = random;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (sleepMillis != -1) {
      return new SlowIndexOutput(super.createOutput(name, context));
    }

    return super.createOutput(name, context);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (sleepMillis != -1) {
      return new SlowIndexInput(super.openInput(name, context));
    }
    return super.openInput(name, context);
  }

  void doSleep(Random random, int length) {
    int sTime = length < 10 ? sleepMillis : (int) (sleepMillis * Math.log(length));
    if (random != null) {
      sTime = random.nextInt(sTime);
    }
    try {
      Thread.sleep(sTime);
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }

  /** Make a private random. */
  Random forkRandom() {
    if (random == null) {
      return null;
    }
    return new Random(random.nextLong());
  }

  /** Delegate class to wrap an IndexInput and delay reading bytes by some specified time. */
  private class SlowIndexInput extends FilterIndexInput {
    private int numRead = 0;
    private Random rand;

    public SlowIndexInput(IndexInput ii) {
      super("SlowIndexInput(" + ii + ")", ii);
      this.rand = forkRandom();
    }

    @Override
    public byte readByte() throws IOException {
      if (numRead >= IO_SLEEP_THRESHOLD) {
        doSleep(rand, 0);
        numRead = 0;
      }
      ++numRead;
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      if (numRead >= IO_SLEEP_THRESHOLD) {
        doSleep(rand, len);
        numRead = 0;
      }
      numRead += len;
      in.readBytes(b, offset, len);
    }

    @Override
    public boolean equals(Object o) {
      return in.equals(o);
    }

    @Override
    public int hashCode() {
      return in.hashCode();
    }
  }

  /** Delegate class to wrap an IndexOutput and delay writing bytes by some specified time. */
  private class SlowIndexOutput extends FilterIndexOutput {

    private int numWrote;
    private final Random rand;

    public SlowIndexOutput(IndexOutput out) {
      super("SlowIndexOutput(" + out + ")", out.getName(), out);
      this.rand = forkRandom();
    }

    @Override
    public void writeByte(byte b) throws IOException {
      if (numWrote >= IO_SLEEP_THRESHOLD) {
        doSleep(rand, 0);
        numWrote = 0;
      }
      ++numWrote;
      out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      if (numWrote >= IO_SLEEP_THRESHOLD) {
        doSleep(rand, length);
        numWrote = 0;
      }
      numWrote += length;
      out.writeBytes(b, offset, length);
    }
  }
}
