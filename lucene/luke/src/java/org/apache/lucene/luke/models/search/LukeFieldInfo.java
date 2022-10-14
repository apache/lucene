package org.apache.lucene.luke.models.search;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;

/** Represents the type of field. Derived from {@link FieldInfo}. */
public final class LukeFieldInfo {

  public final String name;
  public final DocValuesType docValuesType;
  public final IndexOptions indexOptions;

  public LukeFieldInfo(FieldInfo fieldInfo) {
    name = fieldInfo.getName();
    docValuesType = fieldInfo.getDocValuesType();
    indexOptions = fieldInfo.getIndexOptions();
  }
}
