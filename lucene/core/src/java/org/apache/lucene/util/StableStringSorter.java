package org.apache.lucene.util;

import java.util.Comparator;

abstract class StableStringSorter extends StringSorter {

  StableStringSorter(Comparator<BytesRef> cmp) {
    super(cmp);
  }

  protected abstract void save(int i, int j);

  protected abstract void restore(int i, int j);

  @Override
  protected Sorter radixSorter(BytesRefComparator cmp) {
    return new StableMSBRadixSorter(cmp.comparedBytesCount) {

      @Override
      protected void save(int i, int j) {
        StableStringSorter.this.save(i, j);
      }

      @Override
      protected void restore(int i, int j) {
        StableStringSorter.this.restore(i, j);
      }

      @Override
      protected void swap(int i, int j) {
        StableStringSorter.this.swap(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        get(scratch1, scratchBytes1, i);
        return cmp.byteAt(scratchBytes1, k);
      }
    };
  }

  @Override
  protected Sorter fallbackSorter(Comparator<BytesRef> cmp) {
    // TODO: Maybe tim sort is better?
    return new InPlaceMergeSorter() {
      @Override
      protected int compare(int i, int j) {
        return StableStringSorter.this.compare(i, j);
      }

      @Override
      protected void swap(int i, int j) {
        StableStringSorter.this.swap(i, j);
      }
    };
  }
}
