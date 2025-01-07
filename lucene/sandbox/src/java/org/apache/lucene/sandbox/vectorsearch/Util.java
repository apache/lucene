package org.apache.lucene.sandbox.vectorsearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Util {

  public static ByteArrayOutputStream getZipEntryBAOS(String fileName, SegmentInputStream segInputStream)
      throws IOException {
    segInputStream.reset();
    ZipInputStream zipInputStream = new ZipInputStream(segInputStream);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    boolean fileFound = false;
    ZipEntry zipEntry;
    while (zipInputStream.available() == 1 && ((zipEntry = zipInputStream.getNextEntry()) != null)) {
      if (zipEntry.getName().equals(fileName)) {
        fileFound = true;
        byte[] buffer = new byte[1024];
        int length;
        while ((length = zipInputStream.read(buffer)) != -1) {
          baos.write(buffer, 0, length);
        }
      }
    }
    if (!fileFound) throw new FileNotFoundException();
    return baos;
  }

  private static final Logger log = Logger.getLogger(Util.class.getName());

  public static ArrayList<float[]> getMergedVectors(List<SegmentInputStream> segInputStreams, String fieldName, String mergedSegmentName)
      throws IOException {
    ZipEntry zs;
    ArrayList<float[]> mergedVectors = new ArrayList<float[]>();
    log.info("Getting mergedVectors...");
    for (SegmentInputStream segInputStream : segInputStreams) {
      segInputStream.reset();
      ZipInputStream zipStream = new ZipInputStream(segInputStream);
      while ((zs = zipStream.getNextEntry()) != null) {
        log.info("Getting mergedVectors... " + zs.getName());
        byte[] buffer = new byte[1024];
        int length;
        if (zs.getName().endsWith(".vec")) {
          String field = zs.getName().split("\\.")[0].split("/")[1];
          if (fieldName.equals(field)) {
            ByteArrayOutputStream baosM = new ByteArrayOutputStream();
            while ((length = zipStream.read(buffer)) != -1) {
              baosM.write(buffer, 0, length);
            }
            List<float[]> m = deSerializeListInMemory(baosM.toByteArray());
            mergedVectors.addAll(m);
          }
        }
      }
    }
    return mergedVectors;
  }

  public static void getMergedArchiveCOS(List<SegmentInputStream> segInputStreams, String mergedSegmentName,
      OutputStream os) throws IOException {
    ZipOutputStream zos = new ZipOutputStream(os);
    ZipEntry zs;
    Map<String, Integer> mergedMetaMap = new LinkedHashMap<String, Integer>();
    for (SegmentInputStream segInputStream : segInputStreams) {
      segInputStream.reset();
      ZipInputStream zipStream = new ZipInputStream(segInputStream);
      while ((zs = zipStream.getNextEntry()) != null) {
        byte[] buffer = new byte[1024];
        int length;
        if (zs.getName().endsWith(".meta")) {
          ByteArrayOutputStream baosM = new ByteArrayOutputStream();
          while ((length = zipStream.read(buffer)) != -1) {
            baosM.write(buffer, 0, length);
          }
          Map<String, Integer> m = deSerializeMapInMemory(baosM.toByteArray());
          mergedMetaMap.putAll(m);
        } else {
          ZipEntry zipEntry = new ZipEntry(zs.getName());
          zos.putNextEntry(zipEntry);
          zos.setLevel(Deflater.NO_COMPRESSION);
          while ((length = zipStream.read(buffer)) != -1) {
            zos.write(buffer, 0, length);
          }
          zos.closeEntry();
        }
      }
    }
    // Finally put the merged meta file
    ZipEntry mergedMetaZipEntry = new ZipEntry(mergedSegmentName + ".meta");
    zos.putNextEntry(mergedMetaZipEntry);
    zos.setLevel(Deflater.NO_COMPRESSION);
    new ObjectOutputStream(zos).writeObject(mergedMetaMap); // Java serialization should be avoided
    zos.closeEntry();
    zos.close();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Integer> deSerializeMapInMemory(byte[] bytes) {
    Map<String, Integer> map = null;
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      map = (Map<String, Integer>) ois.readObject();
      ois.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return map;
  }

  @SuppressWarnings("unchecked")
  public static List<float[]> deSerializeListInMemory(byte[] bytes) {
    List<float[]> map = null;
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      map = (List<float[]>) ois.readObject();
      ois.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return map;
  }

}
