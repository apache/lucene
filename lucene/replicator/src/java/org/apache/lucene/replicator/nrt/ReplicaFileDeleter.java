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

package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FileDeleter;

class ReplicaFileDeleter {
  private final FileDeleter fileDeleter;
  private final Directory dir;
  private final Node node;
  private final IndexDeletionPolicy policy;

  public ReplicaFileDeleter(Node node, Directory dir, IndexDeletionPolicy policy)
      throws IOException {
    this.dir = dir;
    this.policy = policy;
    this.node = node;
    this.fileDeleter =
        new FileDeleter(
            dir,
            ((msgType, s) -> {
              if (msgType == FileDeleter.MsgType.FILE && Node.VERBOSE_FILES) {
                node.message(s);
              }
            }));

    List<IndexCommit> commits = new ArrayList<>();
    String[] files = dir.listAll();
    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
    for (String fileName : files) {
      m.reset(fileName);
      if (!fileName.endsWith("write.lock")
          && (m.matches()
              || fileName.startsWith(IndexFileNames.SEGMENTS)
              || fileName.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
        // Add this file to refCounts with initial count 0:
        fileDeleter.initRefCount(fileName);
        if (fileName.startsWith(IndexFileNames.SEGMENTS)) {

          // This is a commit (segments or segments_N), and
          // it's valid (<= the max gen).  Load it, then
          // incref all files it refers to:
          node.message("init: load commit \"" + fileName + "\"");
          SegmentInfos sis = SegmentInfos.readCommit(dir, fileName);
          CommitPoint commitPoint = new CommitPoint(dir, sis);
          commits.add(commitPoint);
        }
      }
    }
    this.policy.onInit(commits);
   
  }

  public void checkpoint(SegmentInfos sis) throws IOException {
    CommitPoint commitPoint = new CommitPoint(dir, sis);
    policy.onCommit(Collections.singletonList(commitPoint));
  }
  
  public synchronized void incRef(Collection<String> fileNames) throws IOException {
    fileDeleter.incRef(fileNames);
  }

  public synchronized void decRef(Collection<String> fileNames) throws IOException {
    fileDeleter.decRef(fileNames);

    // TODO: this local IR could incRef files here, like we do now with IW's NRT readers ... then we
    // can assert this again:

    // we can't assert this, e.g a search can be running when we switch to a new NRT point, holding
    // a previous IndexReader still open for
    // a bit:
    /*
    // We should never attempt deletion of a still-open file:
    Set<String> delOpen = ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
    if (delOpen.isEmpty() == false) {
      node.message("fail: we tried to delete these still-open files: " + delOpen);
      throw new AssertionError("we tried to delete these still-open files: " + delOpen);
    }
    */
  }

  public synchronized int getRefCount(String fileName) {
    return fileDeleter.getRefCount(fileName);
  }

  public synchronized void deleteIfNoRef(String fileName) throws IOException {
    fileDeleter.deleteFileIfNoRef(fileName);
  }

  public synchronized void forceDeleteFile(String fileName) throws IOException {
    fileDeleter.forceDelete(fileName);
  }

  public synchronized void deleteUnknownFiles(String segmentsFileName) throws IOException {
    Set<String> toDelete = fileDeleter.getUnrefedFiles();
    for (String fileName : dir.listAll()) {
      if (fileDeleter.exists(fileName) == false
          && fileName.equals("write.lock") == false
          && fileName.equals(segmentsFileName) == false) {
        node.message("will delete unknown file \"" + fileName + "\"");
        toDelete.add(fileName);
      }
    }
    fileDeleter.deleteFilesIfNoRef(toDelete);
  }

  private static final class CommitPoint extends IndexCommit {

    private Directory directory;
    private String segmentsFileName;
    private boolean deleted;
    private Collection<String> files;
    long generation;
    final Map<String, String> userData;
    private final int segmentCount;

    public CommitPoint(Directory directory, SegmentInfos segmentInfos) throws IOException {
      this.directory = directory;
      userData = segmentInfos.getUserData();
      segmentsFileName = segmentInfos.getSegmentsFileName();
      generation = segmentInfos.getGeneration();
      files = Collections.unmodifiableCollection(segmentInfos.files(true));
      segmentCount = segmentInfos.size();
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return directory;
    }

    @Override
    public void delete() {
      if (!deleted) {
        deleted = true;
      }
    }

    @Override
    public boolean isDeleted() {
      return deleted;
    }

    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
      return userData;
    }
  }
}
