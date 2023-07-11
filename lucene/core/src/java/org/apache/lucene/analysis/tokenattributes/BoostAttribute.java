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

import org.apache.lucene.util.Attribute;

/**
 * Add this {@link BoostAttribute} if you want to manipulate the token stream in order to update the
 * boost associated to a token
 *
 * <p><b>Please note:</b> This attribute does not work at index time
 *
 * @lucene.internal
 */
public interface BoostAttribute extends Attribute {
  /** Sets the boost in this attribute */
  public void setBoost(float boost);
  /** Retrieves the boost, default is {@code 1.0f}. */
  public float getBoost();
}
