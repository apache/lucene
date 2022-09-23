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

package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Default implementation of {@link SentenceAttribute}.
 *
 * <p>The current implementation is coincidentally identical to {@link FlagsAttributeImpl} It was
 * decided to keep it separate because this attribute will NOT be an implied bitmap. Also, this
 * class may hold other sentence specific data in the future.
 */
public class SentenceAttributeImpl extends AttributeImpl implements SentenceAttribute {

  private int index = 0;

  /** Initialize this attribute to default */
  public SentenceAttributeImpl() {}

  @Override
  public void clear() {
    index = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other instanceof SentenceAttributeImpl) {
      return ((SentenceAttributeImpl) other).index == index;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return index;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    SentenceAttribute t = (SentenceAttribute) target;
    t.setSentenceIndex(index);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(SentenceAttribute.class, "sentences", index);
  }

  @Override
  public int getSentenceIndex() {
    return index;
  }

  @Override
  public void setSentenceIndex(int sentence) {
    this.index = sentence;
  }
}
