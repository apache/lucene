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
package org.apache.lucene.codecs.monitoring;

import java.io.IOException;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

/** TermVectorsWriter implementation that delegates calls to another TermVectorsWriter. */
public abstract class FilterTermVectorsWriter extends TermVectorsWriter {
  protected final TermVectorsWriter in;

  public FilterTermVectorsWriter(TermVectorsWriter in) {
    this.in = in;
  }

  @Override
  public void startDocument(int i) throws IOException {
    in.startDocument(i);
  }

  @Override
  public void startField(FieldInfo fieldInfo, int i, boolean b, boolean b1, boolean b2)
      throws IOException {
    in.startField(fieldInfo, i, b, b1, b2);
  }

  @Override
  public void startTerm(BytesRef bytesRef, int i) throws IOException {
    in.startTerm(bytesRef, i);
  }

  @Override
  public void addPosition(int i, int i1, int i2, BytesRef bytesRef) throws IOException {
    in.addPosition(i, i1, i2, bytesRef);
  }

  @Override
  public void finish(int i) throws IOException {
    in.finish(i);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long ramBytesUsed() {
    return in.ramBytesUsed();
  }

  @Override
  public void finishDocument() throws IOException {
    in.finishDocument();
  }

  @Override
  public void finishField() throws IOException {
    in.finishField();
  }

  @Override
  public void finishTerm() throws IOException {
    in.finishTerm();
  }

  @Override
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    in.addProx(numProx, positions, offsets);
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    return in.merge(mergeState);
  }
}
