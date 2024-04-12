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

package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.LeafReader;

/**
 * FieldOffsetStrategy that combines offsets from multiple fields. Used to highlight a single field
 * based on matches from multiple fields.
 *
 * @lucene.internal
 */
public class MultiFieldsOffsetStrategy extends FieldOffsetStrategy {
  private final List<FieldOffsetStrategy> fieldsOffsetStrategies;

  public MultiFieldsOffsetStrategy(List<FieldOffsetStrategy> fieldsOffsetStrategies) {
    super(null);
    this.fieldsOffsetStrategies = fieldsOffsetStrategies;
  }

  @Override
  public String getField() {
    throw new IllegalStateException("MultiFieldsOffsetStrategy does not have a single field.");
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    // TODO: what should be returned here as offset source?
    return fieldsOffsetStrategies.get(0).getOffsetSource();
  }

  @Override
  public OffsetsEnum getOffsetsEnum(LeafReader reader, int docId, String content)
      throws IOException {
    List<OffsetsEnum> fieldsOffsetsEnums = new ArrayList<>(fieldsOffsetStrategies.size());
    for (FieldOffsetStrategy fieldOffsetStrategy : fieldsOffsetStrategies) {
      OffsetsEnum offsetsEnum = fieldOffsetStrategy.getOffsetsEnum(reader, docId, content);
      if (offsetsEnum != OffsetsEnum.EMPTY) {
        fieldsOffsetsEnums.add(offsetsEnum);
      }
    }
    return new OffsetsEnum.MultiOffsetsEnum(fieldsOffsetsEnums);
  }
}
