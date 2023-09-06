package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuException;

/** {@link RuntimeException} wrapping a {@link DpuException}. */
public class UncheckedDpuException extends RuntimeException {

  public UncheckedDpuException(DpuException cause) {
    super(cause);
  }
}
