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
import java.util.Iterator;
import java.util.Arrays;

/** An iterator over column values. */
public class ColumnValues<T> implements Iterator<T>, Iterable<T> {
  private final ColumnDescriptor column;
  private final ValueType type;
  private final BlockDescriptor[] blocks;
  private final InputBuffer in;

  private final long[] firstRows;                 // for binary searches

  private int block = 0;
  private long row;

  ColumnValues(ColumnDescriptor column) throws IOException {
    this.column = column;
    this.type = column.metaData.getType();
    this.blocks = column.getBlocks();
    this.in = new InputBuffer(column.file, column.dataStart);

    firstRows = new long[blocks.length];          // initialize firstRows
    for (int i = 0; i < blocks.length; i++)
      firstRows[i] = blocks[i].firstRow;
  }

  /** Return the current row number within this file. */
  public long getRow() { return row; }

  /** Seek to the named row. */
  public void seek(long r) throws IOException {
    if (r < row || r >= blocks[block].lastRow()) { // not in current block
      block = Arrays.binarySearch(firstRows, r);  // find block
      if (block < 0)
        block = -block - 2;
      row = blocks[block].firstRow;               // seek to block
      in.seek(blocks[block].startPosition);
    }
    while (r > row && hasNext()) {                // skip within block
      in.skipValue(type);
      row++;
    }
  }

  @Override public Iterator iterator() { return this; }

  @Override public boolean hasNext() {
    return block < blocks.length-1 || row < blocks[block].lastRow();
  }

  @Override public T next() {
    if (row >= blocks[block].lastRow()) {
      if (block >= blocks.length)
        throw new TrevniRuntimeException("Read past end of column.");
      readChecksum();
      block++;
    }
    row++;
    try {
      return (T)in.readValue(type);
    } catch (IOException e) {
      throw new TrevniRuntimeException(e);
    }
  }

  @Override public void remove() { throw new UnsupportedOperationException(); }

  private void readChecksum() {}

}
  
