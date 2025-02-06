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
package org.apache.lucene.index;

import java.util.Objects;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;

/**
 * A fixed size DataInput which includes the length of the input. For use as a StoredField.
 *
 * @lucene.experimental
 */
public class StoredFieldDataInput {

  private final DataInput in;
  private final int length;

  public StoredFieldDataInput(DataInput in, int length) {
    this.in = Objects.requireNonNull(in);
    this.length = length;
  }

  public StoredFieldDataInput(ByteArrayDataInput byteArrayDataInput) {
    this(byteArrayDataInput, byteArrayDataInput.length());
  }

  public DataInput getDataInput() {
    return in;
  }

  public int getLength() {
    return length;
  }
}
