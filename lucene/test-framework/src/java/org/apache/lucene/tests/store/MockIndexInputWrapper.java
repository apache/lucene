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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.internal.tests.TestSecrets;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * Used by MockDirectoryWrapper to create an input stream that keeps track of when it's been closed.
 */
public class MockIndexInputWrapper extends FilterIndexInput {

  static {
    TestSecrets.getFilterInputIndexAccess().addTestFilterType(MockIndexInputWrapper.class);
  }

  private MockDirectoryWrapper dir;
  final String name;
  private volatile boolean closed;

  // Which MockIndexInputWrapper we were cloned from, or null if we are not a clone:
  private final MockIndexInputWrapper parent;
  private final boolean confined;
  private final Thread thread;

  /** Sole constructor */
  public MockIndexInputWrapper(
      MockDirectoryWrapper dir,
      String name,
      IndexInput delegate,
      MockIndexInputWrapper parent,
      boolean confined) {
    super("MockIndexInputWrapper(name=" + name + " delegate=" + delegate + ")", delegate);

    // If we are a clone then our parent better not be a clone!
    assert parent == null || parent.parent == null;

    this.parent = parent;
    this.name = name;
    this.dir = dir;
    this.confined = confined;
    this.thread = Thread.currentThread();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      in.close(); // don't mask double-close bugs
      return;
    }
    closed = true;

    try (Closeable delegate = in) {
      // Pending resolution on LUCENE-686 we may want to
      // remove the conditional check so we also track that
      // all clones get closed:
      assert delegate != null;
      if (parent == null) {
        dir.removeIndexInput(this, name);
      }
      dir.maybeThrowDeterministicException();
    }
  }

  private void ensureOpen() {
    // TODO: not great this is a volatile read (closed) ... we should deploy heavy JVM voodoo like
    // SwitchPoint to avoid this
    if (closed) {
      throw new RuntimeException("Abusing closed IndexInput!");
    }
    if (parent != null && parent.closed) {
      throw new RuntimeException("Abusing clone of a closed IndexInput!");
    }
  }

  private void ensureAccessible() {
    if (confined && thread != Thread.currentThread()) {
      throw new RuntimeException("Abusing from another thread!");
    }
  }

  @Override
  public MockIndexInputWrapper clone() {
    ensureOpen();
    if (dir.verboseClone) {
      new Exception("clone: " + this).printStackTrace(System.out);
    }
    dir.inputCloneCount.incrementAndGet();
    IndexInput iiclone = in.clone();
    MockIndexInputWrapper clone =
        new MockIndexInputWrapper(dir, name, iiclone, parent != null ? parent : this, confined);
    // Pending resolution on LUCENE-686 we may want to
    // uncomment this code so that we also track that all
    // clones get closed:
    /*
    synchronized(dir.openFiles) {
      if (dir.openFiles.containsKey(name)) {
        Integer v = (Integer) dir.openFiles.get(name);
        v = Integer.valueOf(v.intValue()+1);
        dir.openFiles.put(name, v);
      } else {
        throw new RuntimeException("BUG: cloned file was not open?");
      }
    }
    */
    return clone;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    ensureOpen();
    if (dir.verboseClone) {
      new Exception("slice: " + this).printStackTrace(System.out);
    }
    dir.inputCloneCount.incrementAndGet();
    IndexInput slice = in.slice(sliceDescription, offset, length);
    MockIndexInputWrapper clone =
        new MockIndexInputWrapper(
            dir, sliceDescription, slice, parent != null ? parent : this, confined);
    return clone;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length, IOContext context)
      throws IOException {
    ensureOpen();
    if (dir.verboseClone) {
      new Exception("slice: " + this).printStackTrace(System.out);
    }
    dir.inputCloneCount.incrementAndGet();
    IndexInput slice = in.slice(sliceDescription, offset, length);
    MockIndexInputWrapper clone =
        new MockIndexInputWrapper(
            dir, sliceDescription, slice, parent != null ? parent : this, confined);
    return clone;
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    ensureAccessible();
    return in.getFilePointer();
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.seek(pos);
  }

  @Override
  public void prefetch(long offset, long length) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.prefetch(offset, length);
  }

  @Override
  public Optional<Boolean> isLoaded() {
    ensureOpen();
    ensureAccessible();
    return in.isLoaded();
  }

  @Override
  public void updateIOContext(IOContext context) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.updateIOContext(context);
  }

  @Override
  public long length() {
    ensureOpen();
    return in.length();
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.readBytes(b, offset, len);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public void readFloats(float[] floats, int offset, int len) throws IOException {
    ensureOpen();
    ensureAccessible();
    in.readFloats(floats, offset, len);
  }

  @Override
  public short readShort() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readShort();
  }

  @Override
  public int readInt() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readInt();
  }

  @Override
  public long readLong() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readLong();
  }

  @Override
  public String readString() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readString();
  }

  @Override
  public int readVInt() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readVInt();
  }

  @Override
  public long readVLong() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readVLong();
  }

  @Override
  public int readZInt() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readZInt();
  }

  @Override
  public long readZLong() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readZLong();
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    ensureOpen();
    ensureAccessible();
    super.skipBytes(numBytes);
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readMapOfStrings();
  }

  @Override
  public Set<String> readSetOfStrings() throws IOException {
    ensureOpen();
    ensureAccessible();
    return in.readSetOfStrings();
  }

  @Override
  public String toString() {
    return "MockIndexInputWrapper(" + in + ")";
  }
}
