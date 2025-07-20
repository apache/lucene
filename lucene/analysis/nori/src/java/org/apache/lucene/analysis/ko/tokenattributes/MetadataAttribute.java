package org.apache.lucene.analysis.ko.tokenattributes;

import org.apache.lucene.analysis.ko.Token;
import org.apache.lucene.util.Attribute;

/**
 * Attribute for Korean token metadata.
 *
 * <p>This attribute provides access to additional metadata associated with Korean tokens,
 * particularly from user dictionaries and compound word morphemes.
 *
 * <p>Note: in some cases this value may not be applicable, and will be null.
 *
 * @lucene.experimental
 */
public interface MetadataAttribute extends Attribute {
  /** Get the metadata string of the token. */
  String getMetadata();

  /** Set the current token. */
  void setToken(Token token);
}
