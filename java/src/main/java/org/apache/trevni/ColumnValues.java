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

/** */
public class ColumnValues<T> implements Iterator<T>, Iterable<T> {
  private final ColumnDescriptor column;
  private final ValueType type;
  private final BlockDescriptor[] blocks;
  private final InputBuffer in;

  private int block = 0;

  public ColumnValues(ColumnDescriptor column) throws IOException {
    this.column = column;
    this.type = column.metaData.getType();
    this.blocks = column.getBlocks();
    this.in = new InputBuffer(column.file, column.dataStart);
  }

  public Iterator iterator() { return this; }

  public boolean hasNext() {
    return block != blocks.length-1
      || in.valueCount() != blocks[block].valueCount;
  }

  public T next() {
    if (in.valueCount() >= blocks[block].valueCount) {
      if (block >= blocks.length)
        throw new TrevniRuntimeException("Read past end of column.");
      readChecksum();
      block++;
      in.valueCount(0);
    }
    try {
      return (T)in.readValue(type);
    } catch (IOException e) {
      throw new TrevniRuntimeException(e);
    }
  }

  public void remove() { throw new UnsupportedOperationException(); }

  private void readChecksum() {}

}
  
