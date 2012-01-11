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
import java.io.FileOutputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.List;

/** Writes data to a column file.
 * All data is buffered until {@link #writeTo(File)} is called.
 */
public class ColumnFileWriter {

  static final byte[] MAGIC = new byte[] {'T', 'r', 'v', 0};

  private ColumnFileMetaData metaData = new ColumnFileMetaData();
  private ColumnMetaData[] columns;
  private List<OutputBuffer>[] blocks;

  private long rowCount;
  private int columnCount;

  /** Construct given metadata for each column in the file. */
  public ColumnFileWriter(ColumnMetaData... columns) throws IOException {
    this.columns = columns;
    this.columnCount = columns.length;
    this.blocks = new List[columnCount];
    for (int i = 0; i < columnCount; i++) {
      blocks[i] = new ArrayList<OutputBuffer>();
      blocks[i].add(new OutputBuffer());
    }
  }

  /** Return the file's metadata. */
  public ColumnFileMetaData getMetaData() { return metaData; }

  /** Add a row to the file. */
  public void writeRow(Object... row) throws IOException {
    for (int column = 0; column < columnCount; column++)
      getLastBlock(column).writeValue(row[column], columns[column].getType());
    rowCount++;
  }

  private OutputBuffer getLastBlock(int column) {
    List<OutputBuffer> all = blocks[column];
    OutputBuffer last = all.get(all.size()-1);
    if (last.isFull()) {                          // last is full
      last = new OutputBuffer();                  // add a new block to column
      all.add(last);
    }
    return last;
  }

  /** Write all rows added to the named file. */
  public void writeTo(File file) throws IOException {
    OutputStream out = new FileOutputStream(file);
    try {
      writeTo(out);
    } finally {
      out.close();
    }
  }

  /** Write all rows added to the named output stream. */
  public void writeTo(OutputStream out) throws IOException {
    writeHeader(out);
    
    for (int column = 0; column < columnCount; column++)
      writeColumn(columns[column], blocks[column], out);
  }

  private void writeHeader(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();

    header.write(MAGIC);                          // magic

    header.writeFixed64(rowCount);                // row count

    header.writeFixed32(columnCount);             // column count

    metaData.write(header);                       // file metadata

    for (ColumnMetaData column : columns)
      column.write(header);                       // column metadata

    for (long start : computeStarts(header.size()))
      header.writeFixed64(start);                 // column starts

    header.writeTo(out);

  }

  private long[] computeStarts(long start) {
    long[] result = new long[columnCount];
    start += columnCount * 8;                     // room for starts
    for (int column = 0; column < columnCount; column++) {
      result[column] = start;
      start += 4;                                 // block count
      for (OutputBuffer block : blocks[column]) {
        start += 4 * 3;                           // count & sizes
        start += block.size();
      }
    }
    return result;
  }

  private void writeColumn(ColumnMetaData column,
                           List<OutputBuffer> blocks,
                           OutputStream out)
    throws IOException {

    OutputBuffer header = new OutputBuffer();

    header.writeFixed32(blocks.size());
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

    header.writeFixed32(block.getRowCount());   // number of rows
    header.writeFixed32(block.size());          // uncompressed size
    header.writeFixed32(block.size());          // compressed size
  }
}

