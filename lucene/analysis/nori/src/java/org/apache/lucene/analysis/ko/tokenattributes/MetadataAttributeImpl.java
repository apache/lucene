package org.apache.lucene.analysis.ko.tokenattributes;

import org.apache.lucene.analysis.ko.Token;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Attribute implementation for Korean token metadata.
 *
 * @lucene.experimental
 */
public class MetadataAttributeImpl extends AttributeImpl implements MetadataAttribute {
  private Token token;

  @Override
  public String getMetadata() {
    if (this.token != null) {
      return this.token.getMetadata();
    }
    return null;
  }

  @Override
  public void setToken(Token token) {
    this.token = token;
  }

  @Override
  public void clear() {
    this.token = null;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    final MetadataAttribute t = (MetadataAttribute) target;
    t.setToken(this.token);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(MetadataAttribute.class, "metadata", getMetadata());
  }
}
