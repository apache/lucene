package org.apache.lucene.index;

import org.apache.lucene.internal.tests.IndexPackageSecrets;

/**
 * Exposes certain package-private secrets to the test framework.
 *
 * @lucene.internal
 */
public final class PackageSecrets {
  private PackageSecrets() {}

  /**
   * This method returns a test accessor that exposes some internals to test infrastructure.
   * Everything here is internal, subject to change without notice and not publicly accessible.
   *
   * <p>Within the test infrastructure, do not call this method directly, instead use the static
   * factory methods on the corresponding {@code TestSecrets} class.
   *
   * @param accessToken A secret token to only permit instantiation via the corresponding secrets
   *     class.
   * @return An instance of the secrets class; the return type is hidden from the public API.
   * @lucene.internal
   */
  public static final Object getTestSecrets(Object accessToken) {
    return new IndexPackageSecrets(accessToken) {

      @Override
      public IndexReader.CacheKey newCacheKey() {
        return new IndexReader.CacheKey();
      }

      @Override
      public void setIndexWriterMaxDocs(int limit) {
        IndexWriter.setMaxDocs(limit);
      }

      @Override
      public FieldInfosBuilder newFieldInfosBuilder(String softDeletesFieldName) {
        return new FieldInfosBuilder() {
          private FieldInfos.Builder builder =
              new FieldInfos.Builder(new FieldInfos.FieldNumbers(softDeletesFieldName));

          @Override
          public FieldInfosBuilder add(FieldInfo fi) {
            builder.add(fi);
            return this;
          }

          @Override
          public FieldInfos finish() {
            return builder.finish();
          }
        };
      }
    };
  }
}
