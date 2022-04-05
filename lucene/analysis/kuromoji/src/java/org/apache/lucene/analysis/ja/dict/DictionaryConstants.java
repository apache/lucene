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
package org.apache.lucene.analysis.ja.dict;

/** Dictionary constants */
final class DictionaryConstants {
  /** Codec header of the dictionary file. */
  public static final String DICT_HEADER = "kuromoji_dict";
  /** Codec header of the dictionary mapping file. */
  public static final String TARGETMAP_HEADER = "kuromoji_dict_map";
  /** Codec header of the POS dictionary file. */
  public static final String POSDICT_HEADER = "kuromoji_dict_pos";
  /** Codec header of the connection costs. */
  public static final String CONN_COSTS_HEADER = "kuromoji_cc";
  /** Codec header of the character definition file. */
  public static final String CHARDEF_HEADER = "kuromoji_cd";
  /** Codec version of the binary dictionary */
  public static final int VERSION = 1;
}
