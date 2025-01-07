package org.apache.lucene.sandbox.vectorsearch;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CuVSSegmentFile implements AutoCloseable{
  final private ZipOutputStream zos;

  private Set<String> filesAdded = new HashSet<String>();

  public CuVSSegmentFile(OutputStream out) {
    zos = new ZipOutputStream(out);
    zos.setLevel(Deflater.NO_COMPRESSION);
  }
  
  protected Logger log = Logger.getLogger(getClass().getName());

  public void addFile(String name, byte[] bytes) throws IOException {
    log.info("Writing the file: " + name + ", size="+bytes.length + ", space remaining: "+new File("/").getFreeSpace());
    ZipEntry indexFileZipEntry = new ZipEntry(name);
    zos.putNextEntry(indexFileZipEntry);
    zos.write(bytes, 0, bytes.length);
    zos.closeEntry();
    filesAdded.add(name);
  }
  
  public Set<String> getFilesAdded() {
    return Collections.unmodifiableSet(filesAdded);
  }

  @Override
  public void close() throws IOException {
    zos.close();
  }
}
