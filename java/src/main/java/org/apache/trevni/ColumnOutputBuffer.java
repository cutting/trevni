/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ColumnOutputBuffer {
  private ColumnMetaData meta;
  private Codec codec;
  private Checksum checksum;
  private OutputBuffer buffer;
  private List<BlockDescriptor> blockDescriptors;
  private List<byte[]> blockData;

  public ColumnOutputBuffer(ColumnMetaData meta) throws IOException {
    this.meta = meta;
    this.codec = Codec.get(meta);
    this.checksum = Checksum.get(meta);
    this.buffer = new OutputBuffer();
    this.blockDescriptors = new ArrayList<BlockDescriptor>();
    this.blockData = new ArrayList<byte[]>();
  }

  public ColumnMetaData getMeta() { return meta; }

  public void writeValue(Object value) throws IOException {
    if (buffer.isFull())
      flushBuffer();
    buffer.writeValue(value, meta.getType());
  }

  public void flushBuffer() throws IOException {
    if (buffer.getRowCount() == 0) return;
    ByteBuffer raw = buffer.asByteBuffer();
    ByteBuffer c = codec.compress(raw);

    blockDescriptors.add(new BlockDescriptor(buffer.getRowCount(),
                                             raw.remaining(),
                                             c.remaining()));

    ByteBuffer data = ByteBuffer.allocate(c.remaining() + checksum.size());
    data.put(c);
    data.put(checksum.compute(raw));
    blockData.add(data.array());

    buffer = new OutputBuffer();
  }

  public long size() throws IOException {
    flushBuffer();
    long size = 4;                                // count of blocks
    size += 4 * 3 * blockDescriptors.size();      // descriptors
    for (byte[] data : blockData)
      size += data.length;                        // data
    return size;
  }

  public void writeTo(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();
    header.writeFixed32(blockDescriptors.size());
    for (BlockDescriptor descriptor: blockDescriptors)
      descriptor.writeTo(header);
    header.writeTo(out);

    for (byte[] data : blockData)
      out.write(data);
  }

}
