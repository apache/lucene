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

package org.apache.lucene.index.memory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

class StoredValues {

  private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private final DataOutput out = new OutputStreamDataOutput(bytes);

  private int count = 0;

  void store(IndexableField field) {
    count++;
    try {
      BytesRef binaryValue = field.binaryValue();
      if (binaryValue != null) {
        out.writeInt(1);
        out.writeInt(binaryValue.length);
        out.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
        return;
      }
      Number numberValue = field.numericValue();
      if (numberValue != null) {
        if (numberValue instanceof Double d) {
          out.writeInt(3);
          out.writeLong(NumericUtils.doubleToSortableLong(d));
          return;
        }
        if (numberValue instanceof Float f) {
          out.writeInt(4);
          out.writeInt(NumericUtils.floatToSortableInt(f));
          return;
        }
        if (numberValue instanceof Long l) {
          out.writeInt(5);
          out.writeLong(l);
          return;
        }
        if (numberValue instanceof Integer i) {
          out.writeInt(6);
          out.writeInt(i);
          return;
        }
      }
      String stringValue = field.stringValue();
      if (stringValue != null) {
        out.writeInt(2);
        out.writeString(stringValue);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e); // entirely in memory so shouldn't happen!
    }
  }

  void retrieve(StoredFieldVisitor visitor, FieldInfo fi) throws IOException {
    DataInput di = new ByteArrayDataInput(bytes.toByteArray());
    for (int i = 0; i < count; i++) {
      int type = di.readInt();
      switch (type) {
        case 1:
          int len = di.readInt();
          byte[] bytes = new byte[len];
          di.readBytes(bytes, 0, len);
          visitor.binaryField(fi, bytes);
          continue;
        case 2:
          String value = di.readString();
          visitor.stringField(fi, value);
          continue;
        case 3:
          visitor.doubleField(fi, NumericUtils.sortableLongToDouble(di.readLong()));
          continue;
        case 4:
          visitor.floatField(fi, NumericUtils.sortableIntToFloat(di.readInt()));
          continue;
        case 5:
          visitor.longField(fi, di.readLong());
          continue;
        case 6:
          visitor.intField(fi, di.readInt());
          continue;
        default:
          throw new IllegalStateException("Unknown stored field encoding type [" + type + "]");
      }
    }
  }
}
