package org.apache.lucene.analysis.morph;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.store.DataOutput;

public abstract class DictionaryEntryWriter {

  protected ByteBuffer buffer;
  protected final List<String> posDict;

  protected DictionaryEntryWriter(int size) {
    this.buffer = ByteBuffer.allocate(size);
    this.posDict = new ArrayList<>();
  }

  protected abstract int putEntry(String[] entry);

  protected abstract void writePosDict(OutputStream bos, DataOutput out) throws IOException;

  void writeDictionary(OutputStream bos, DataOutput out) throws IOException {
    out.writeVInt(buffer.position());
    final WritableByteChannel channel = Channels.newChannel(bos);
    // Write Buffer
    buffer.flip(); // set position to 0, set limit to current position
    channel.write(buffer);
    assert buffer.remaining() == 0L;
  }

  public int currentPosition() {
    return buffer.position();
  }
}
