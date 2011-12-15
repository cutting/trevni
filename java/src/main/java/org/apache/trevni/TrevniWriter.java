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
import java.io.File;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.List;

/** */
public class TrevniWriter {

  static final byte[] MAGIC = new byte[] {'C', 'o', 'l', 0};
  static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

  private ColumnMetaData[] columns;
  private List<OutputBuffer>[] blocks;
  private long rows;

  public TrevniWriter(ColumnMetaData... columns) throws IOException {
    this.columns = columns;
    this.blocks = new ArrayList<OutputBuffer>[columns.size];
    for (int i = 0; i < columns.length; i++) {
      blocks[i] = new OutputBuffer();
    }
  }

  public void writeRow(Object... row) throws IOException {
    for (int column = 0; column < columns.length; column++)
      getLastBlock(column).addValue(row[i]);
    rows++;
  }

  private OutputBuffer getLastBlock(int column) {
    List<OutputBuffer> blocks = blocks[column];
    OutputBuffer last = blocks.get(blocks.size()-1);
    if (last.isFull()) {                          // last is full
      last = new OutputBuffer();                  // add a new block to column
      blocks.add(last);
    }
  }

  /** Write a column file. */
  public void writeTo(OutputStream out) throws IOException {
    writeHeader(out);
    
    for (int column = 0; column < columns.length; column++)
      writeColumn(columns[i], buffers[i], out);
  }

  private void writeHeader(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();

    header.write(MAGIC);                          // magic

    header.writeFixed64(rows);                    // row count

    header.writeFixed64(columns.length);          // column count
    for (ColumnMetaData column : columns)
      column.write(header);                       // column metadata

    for (long start : computeStarts(header.size()))
      header.writeFixed64(start);                 // column starts

    header.writeTo(out);

  }

  private long[] computeStarts(long start) {
    start += columns.length * 8;                  // room for starts

    long[] result = new long[columns.length];
    for (int column = 0; column < columns.length; column++) {
      starts[column] = start;
      start += 8;                                 // block count
      for (OutputBuffer block : blocks[column]) {
        start += 8 * 3;                           // count & sizes
        start += block.size();
      }
    }
  }

  private void writeColumn(ColumnMetaData column,
                           List<OutputBuffer> blocks,
                           OutputStream out)
    throws IOException {

    OutputBuffer header = new OutputBuffer();

    header.writeFixed64(blocks.size());
    for (OutputBuffer block : blocks)
      writeBlockDescriptor(column, block, header);
    header.writeTo(out);

    for (OutputBuffer block : blocks)
      block.writeTo(out);
      
  }

  private void writeBlockDescriptor(ColumnMetaData column,
                                    OutputBuffer block,
                                    OutputBuffer header)
    throws IOException {

    header.writeFixed64(block.getValueCount()); // value count
    header.writeFixed64(block.size());          // uncompressed size
    header.writeFixed64(block.size());          // compressed size
  }
}

