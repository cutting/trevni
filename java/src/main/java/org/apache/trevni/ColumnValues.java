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
import java.nio.ByteBuffer;
import java.util.Iterator;

/** An iterator over column values. */
public class ColumnValues<T> implements Iterator<T>, Iterable<T> {
  private final ColumnDescriptor column;
  private final ValueType type;
  private final Codec codec;
  private final InputBuffer in;

  private InputBuffer values;
  private int block = -1;
  private long row = 0;

  ColumnValues(ColumnDescriptor column) throws IOException {
    this.column = column;
    this.type = column.metaData.getType();
    this.codec = Codec.get(column.metaData);
    this.in = new InputBuffer(column.file);

    column.ensureBlocksRead();
  }

  /** Return the current row number within this file. */
  public long getRow() { return row; }

  /** Seek to the named row. */
  public void seek(long r) throws IOException {
    if (r < row || r >= column.lastRow(block))    // not in current block
      startBlock(column.findBlock(r));            // seek to block start
    while (r > row && hasNext()) {                // skip within block
      values.skipValue(type);
      row++;
    }
  }

  private void startBlock(int block) throws IOException {
    this.block = block;
    this.row = column.firstRows[block];

    in.seek(column.blockStarts[block]);
    byte[] raw = new byte[column.blocks[block].compressedSize];
    in.readFully(raw);
    ByteBuffer data = codec.decompress(ByteBuffer.wrap(raw));
    values = new InputBuffer(new InputBytes(data));
  }

  @Override public Iterator iterator() { return this; }

  @Override public boolean hasNext() {
    return block < column.blockCount()-1 || row < column.lastRow(block);
  }

  @Override public T next() {
    try {
      if (row >= column.lastRow(block)) {
        if (block >= column.blockCount())
          throw new TrevniRuntimeException("Read past end of column.");
        readChecksum();
        startBlock(block+1);
      }
      row++;
      return (T)values.readValue(type);
    } catch (IOException e) {
      throw new TrevniRuntimeException(e);
    }
  }

  @Override public void remove() { throw new UnsupportedOperationException(); }

  private void readChecksum() {}

}
  
