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
package org.apache.lucene.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;

/**
 * This class provides ability to track the reference counts of a set of index files and delete them
 * when their counts decreased to 0.
 *
 * <p>This class is NOT thread-safe, the user should make sure the thread-safety themselves
 *
 * @lucene.internal
 */
public final class FileDeleter {

  private final Map<String, RefCount> refCounts = new HashMap<>();

  private final Directory directory;

  /*
   * user specified message consumer, first argument will be message type
   * second argument will be the actual message
   */
  private final BiConsumer<MsgType, String> messenger;

  /*
   * used to return 0 ref count
   */
  private static final RefCount ZERO_REF = new RefCount("");

  /**
   * Create a new FileDeleter with a messenger consumes various verbose messages
   *
   * @param directory the index directory
   * @param messenger two arguments will be passed in, {@link MsgType} and the actual message in
   *     String. Can be null if the user do not want debug infos
   */
  public FileDeleter(Directory directory, BiConsumer<MsgType, String> messenger) {
    this.directory = directory;
    this.messenger = messenger;
  }

  /**
   * Types of messages this file deleter will broadcast REF: messages about reference FILE: messages
   * about file
   */
  public enum MsgType {
    REF,
    FILE
  }

  public void incRef(Collection<String> fileNames) {
    for (String file : fileNames) {
      incRef(file);
    }
  }

  public void incRef(String fileName) {
    RefCount rc = getRefCountInternal(fileName);
    if (messenger != null) {
      messenger.accept(MsgType.REF, "IncRef \"" + fileName + "\": pre-incr count is " + rc.count);
    }
    rc.incRef();
  }

  /**
   * Decrease ref counts for all provided files, delete them if ref counts down to 0, even on
   * exception. Throw first exception hit, if any
   */
  public void decRef(Collection<String> fileNames) throws IOException {
    Set<String> toDelete = new HashSet<>();
    Throwable firstThrowable = null;
    for (String fileName : fileNames) {
      try {
        if (decRef(fileName)) {
          toDelete.add(fileName);
        }
      } catch (Throwable t) {
        firstThrowable = IOUtils.useOrSuppress(firstThrowable, t);
      }
    }

    try {
      delete(toDelete);
    } catch (Throwable t) {
      firstThrowable = IOUtils.useOrSuppress(firstThrowable, t);
    }

    if (firstThrowable != null) {
      throw IOUtils.rethrowAlways(firstThrowable);
    }
  }

  /** Returns true if the file should be deleted */
  private boolean decRef(String fileName) {
    RefCount rc = getRefCountInternal(fileName);
    if (messenger != null) {
      messenger.accept(MsgType.REF, "DecRef \"" + fileName + "\": pre-decr count is " + rc.count);
    }
    if (rc.decRef() == 0) {
      refCounts.remove(fileName);
      return true;
    }
    return false;
  }

  private RefCount getRefCountInternal(String fileName) {
    return refCounts.computeIfAbsent(fileName, RefCount::new);
  }

  /** if the file is not yet recorded, this method will create a new RefCount object with count 0 */
  public void initRefCount(String fileName) {
    refCounts.computeIfAbsent(fileName, RefCount::new);
  }

  /**
   * get ref count for a provided file, if the file is not yet recorded, this method will return 0
   */
  public int getRefCount(String fileName) {
    return refCounts.getOrDefault(fileName, ZERO_REF).count;
  }

  /** get all files, some of them may have ref count 0 */
  public Set<String> getAllFiles() {
    return refCounts.keySet();
  }

  /** return true only if file is touched and also has larger than 0 ref count */
  public boolean exists(String fileName) {
    return refCounts.containsKey(fileName) && refCounts.get(fileName).count > 0;
  }

  /** get files that are touched but not incref'ed */
  public Set<String> getUnrefedFiles() {
    Set<String> unrefed = new HashSet<>();
    for (var entry : refCounts.entrySet()) {
      RefCount rc = entry.getValue();
      String fileName = entry.getKey();
      if (rc.count == 0) {
        messenger.accept(MsgType.FILE, "removing unreferenced file \"" + fileName + "\"");
        unrefed.add(fileName);
      }
    }
    return unrefed;
  }

  /** delete only files that are unref'ed */
  public void deleteFilesIfNoRef(Collection<String> files) throws IOException {
    Set<String> toDelete = new HashSet<>();
    for (final String fileName : files) {
      // NOTE: it's very unusual yet possible for the
      // refCount to be present and 0: it can happen if you
      // open IW on a crashed index, and it removes a bunch
      // of unref'd files, and then you add new docs / do
      // merging, and it reuses that segment name.
      // TestCrash.testCrashAfterReopen can hit this:
      if (exists(fileName) == false) {
        if (messenger != null) {
          messenger.accept(MsgType.FILE, "will delete new file \"" + fileName + "\"");
        }
        toDelete.add(fileName);
      }
    }

    delete(toDelete);
  }

  public void forceDelete(String fileName) throws IOException {
    refCounts.remove(fileName);
    delete(fileName);
  }

  public void deleteFileIfNoRef(String fileName) throws IOException {
    if (exists(fileName) == false) {
      if (messenger != null) {
        messenger.accept(MsgType.FILE, "will delete new file \"" + fileName + "\"");
      }
      delete(fileName);
    }
  }

  private void delete(Collection<String> toDelete) throws IOException {
    if (messenger != null) {
      messenger.accept(MsgType.FILE, "now delete " + toDelete.size() + " files: " + toDelete);
    }

    // First pass: delete any segments_N files.  We do these first to be certain stale commit points
    // are removed
    // before we remove any files they reference, in case we crash right now:
    for (String fileName : toDelete) {
      assert exists(fileName) == false;
      if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
        delete(fileName);
      }
    }

    // Only delete other files if we were able to remove the segments_N files; this way we never
    // leave a corrupt commit in the index even in the presense of virus checkers:
    for (String fileName : toDelete) {
      assert exists(fileName) == false;
      if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
        delete(fileName);
      }
    }
  }

  private void delete(String fileName) throws IOException {
    try {
      directory.deleteFile(fileName);
    } catch (NoSuchFileException | FileNotFoundException e) {
      if (Constants.WINDOWS) {
        // TODO: can we remove this OS-specific hacky logic?  If windows deleteFile is buggy, we
        // should instead contain this workaround in
        // a WindowsFSDirectory ...
        // LUCENE-6684: we suppress this assert for Windows, since a file could be in a confusing
        // "pending delete" state, where we already
        // deleted it once, yet it still shows up in directory listings, and if you try to delete it
        // again you'll hit NSFE/FNFE:
      } else {
        throw e;
      }
    }
  }

  /** Tracks the reference count for a single index file: */
  public static final class RefCount {

    // fileName used only for better assert error messages
    final String fileName;
    boolean initDone;

    RefCount(String fileName) {
      this.fileName = fileName;
    }

    int count;

    public int incRef() {
      if (initDone == false) {
        initDone = true;
      } else {
        assert count > 0
            : Thread.currentThread().getName()
                + ": RefCount is 0 pre-increment for file \""
                + fileName
                + "\"";
      }
      return ++count;
    }

    public int decRef() {
      assert count > 0
          : Thread.currentThread().getName()
              + ": RefCount is 0 pre-decrement for file \""
              + fileName
              + "\"";
      return --count;
    }
  }
}
