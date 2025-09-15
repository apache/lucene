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
package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import org.apache.lucene.backward_codecs.util.GroupVIntUtil;
import org.apache.lucene.store.DataOutput;

/**
 * Utility methods for DataOutput operations that are only used by backward codecs.
 *
 * @lucene.internal
 */
public final class DataOutputUtil {

  private DataOutputUtil() {} // no instance

  /**
   * Encode integers using group-varint. It uses {@link DataOutput#writeVInt VInt} to encode tail
   * values that are not enough for a group. we need a long[] because this is what postings are
   * using, all longs are actually required to be integers.
   *
   * @param values the values to write
   * @param limit the number of values to write.
   * @lucene.experimental
   */
  public static void writeGroupVInts(DataOutput out, long[] values, int limit) throws IOException {
    byte[] groupVIntBytes = new byte[GroupVIntUtil.MAX_LENGTH_PER_GROUP];
    GroupVIntUtil.writeGroupVInts(out, groupVIntBytes, values, limit);
  }
}