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

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.util.Bits;

/**
 * API for reading stored fields.
 *
 * <p><b>NOTE</b>: This class is not thread-safe and should only be consumed in the thread where it
 * was acquired.
 */
public abstract class StoredFields {
  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected StoredFields() {}

  /**
   * Returns the stored fields of the <code>n</code><sup>th</sup> <code>Document</code> in this
   * index. This is just sugar for using {@link DocumentStoredFieldVisitor}.
   *
   * <p><b>NOTE:</b> for performance reasons, this method does not check if the requested document
   * is deleted, and therefore asking for a deleted document may yield unspecified results. Usually
   * this is not required, however you can test if the doc is deleted by checking the {@link Bits}
   * returned from {@link MultiBits#getLiveDocs}.
   *
   * <p><b>NOTE:</b> only the content of a field is returned, if that field was stored during
   * indexing. Metadata like boost, omitNorm, IndexOptions, tokenized, etc., are not preserved.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  // TODO: we need a separate StoredField, so that the
  // Document returned here contains that class not
  // IndexableField
  public final Document document(int docID) throws IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
    document(docID, visitor);
    return visitor.getDocument();
  }

  /**
   * Expert: visits the fields of a stored document, for custom processing/loading of each field. If
   * you simply want to load all fields, use {@link #document(int)}. If you want to load a subset,
   * use {@link DocumentStoredFieldVisitor}.
   */
  public abstract void document(int docID, StoredFieldVisitor visitor) throws IOException;

  /**
   * Like {@link #document(int)} but only loads the specified fields. Note that this is simply sugar
   * for {@link DocumentStoredFieldVisitor#DocumentStoredFieldVisitor(Set)}.
   */
  public final Document document(int docID, Set<String> fieldsToLoad) throws IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(fieldsToLoad);
    document(docID, visitor);
    return visitor.getDocument();
  }
}
